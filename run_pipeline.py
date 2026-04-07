"""
PC & Electronics Price Tracker — pipeline entry point.

Usage
-----
    # Run all enabled sources and categories (default Azure upload)
    python run_pipeline.py

    # Restrict to specific sources / categories
    python run_pipeline.py --sources amazon_de ebay_de --categories laptops gpus

    # Pick a scraper approach; save locally and upload to Azure
    python run_pipeline.py --scraper bs4 --upload

    # Dry-run: scrape real data, save locally, skip Azure upload
    python run_pipeline.py --scraper bs4 --upload --dry-run

    # Save locally only (no Azure)
    python run_pipeline.py --scraper bs4 --local

    # Run the scraper benchmark (all approaches)
    python run_pipeline.py --benchmark

    # Benchmark a single approach
    python run_pipeline.py --benchmark --scraper serpapi
"""
from __future__ import annotations

import importlib
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import click
import yaml

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Scraper-approach → (module, {source_key: class_name}) registry ─────────────

_APPROACH_MAP: dict[str, tuple[str, dict[str, str]]] = {
    "bs4": (
        "scrapers.beautifulsoup_scraper",
        {
            "amazon_de": "AmazonDEScraper",
            "ebay_de":   "EbayDEScraper",
            "idealo_de": "IdealoDEScraper",
        },
    ),
    "selenium": (
        "scrapers.selenium_scraper",
        {
            "amazon_de": "AmazonDESeleniumScraper",
            "ebay_de":   "EbayDESeleniumScraper",
            "idealo_de": "IdealoDESeleniumScraper",
        },
    ),
    "playwright": (
        "scrapers.playwright_scraper",
        {
            "amazon_de": "AmazonDEPlaywrightScraper",
            "ebay_de":   "EbayDEPlaywrightScraper",
            "idealo_de": "IdealoDEPlaywrightScraper",
        },
    ),
    "crawl4ai": (
        "scrapers.crawl4ai_scraper",
        {
            "amazon_de": "AmazonDECrawl4AIScraper",
            "ebay_de":   "EbayDECrawl4AIScraper",
            "idealo_de": "IdealoDECrawl4AIScraper",
        },
    ),
    "serpapi": (
        "scrapers.serpapi_scraper",
        {
            "google_shopping":   "SerpAPIGoogleShoppingScraper",
            "idealo_de_serpapi": "SerpAPIIdealoDEScraper",
        },
    ),
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_scraper(source_key: str, source_cfg: dict[str, Any], approach: str | None = None):
    """
    Dynamically import and instantiate the scraper for *source_key*.

    If *approach* is given, the class is looked up in ``_APPROACH_MAP``
    rather than from sources.yaml's ``scraper_class`` field.
    """
    source_cfg = dict(source_cfg)
    source_cfg["source_key"] = source_key

    if approach:
        if approach not in _APPROACH_MAP:
            raise ValueError(f"Unknown approach '{approach}'")
        module_name, class_map = _APPROACH_MAP[approach]
        if source_key not in class_map:
            raise ValueError(f"Approach '{approach}' has no class for source '{source_key}'")
        class_name = class_map[source_key]
    else:
        class_name = source_cfg["scraper_class"]
        module_name = f"scrapers.{source_key}"

    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
    except (ModuleNotFoundError, AttributeError) as exc:
        logger.error("scraper_load_failed", source=source_key, error=str(exc))
        raise

    # SerpAPI scrapers don't need a source_cfg dict (they use defaults internally)
    if approach == "serpapi":
        return cls()
    return cls(source_cfg)


# ── Normal pipeline run ────────────────────────────────────────────────────────

def run(
    sources: list[str],
    categories: list[str],
    dry_run: bool,
    use_local: bool,
    upload: bool,
    approach: str | None,
) -> None:
    sources_cfg: dict = load_yaml(settings.SOURCES_CONFIG)
    products_cfg: dict = load_yaml(settings.PRODUCTS_CONFIG)
    run_date: date = datetime.now(timezone.utc).date()

    # When using an approach that has its own sources (serpapi), override source list
    if approach and approach in _APPROACH_MAP:
        _, class_map = _APPROACH_MAP[approach]
        # Only keep sources that this approach supports (and that were requested)
        approach_sources = list(class_map.keys())
        if sources == list(sources_cfg.keys()):   # no explicit --sources flag
            sources = approach_sources
        else:
            sources = [s for s in sources if s in approach_sources]

    logger.info(
        "pipeline_started",
        run_date=str(run_date),
        sources=sources,
        categories=categories,
        approach=approach or "sources.yaml",
        dry_run=dry_run,
        upload=upload,
    )

    # Resolve storage
    local_storage = None
    blob_uploader = None

    if use_local or upload:
        from storage.local_storage import LocalStorage
        local_storage = LocalStorage()

    if upload and not dry_run:
        from storage.blob_uploader import BlobUploader
        blob_uploader = BlobUploader()   # falls back to dry_run=True if creds missing

    total_records = 0

    for source_key in sources:
        # For standard sources, check sources.yaml; SerpAPI sources have no yaml entry
        if approach != "serpapi" and source_key not in sources_cfg:
            logger.warning("unknown_source", source=source_key)
            continue
        if approach != "serpapi" and not sources_cfg.get(source_key, {}).get("enabled", True):
            logger.info("source_disabled", source=source_key)
            continue

        source_cfg = sources_cfg.get(source_key, {"base_url": "https://serpapi.com", "currency": "EUR", "rate_limit_delay": 1})

        try:
            scraper = load_scraper(source_key, source_cfg, approach=approach)
        except Exception as exc:
            logger.warning("skipping_source", source=source_key, error=str(exc))
            continue

        for category in categories:
            if category not in products_cfg:
                logger.warning("unknown_category", category=category)
                continue

            records: list[dict[str, Any]] = []

            for product in products_cfg[category]:
                logger.info(
                    "scraping_product",
                    source=source_key,
                    category=category,
                    product=product.get("name"),
                )
                try:
                    hits = scraper.search(product, category)
                    records.extend(hits)
                    logger.info(
                        "product_scraped",
                        source=source_key,
                        category=category,
                        product=product.get("name"),
                        hits=len(hits),
                    )
                except Exception as exc:
                    logger.error(
                        "product_scrape_failed",
                        source=source_key,
                        category=category,
                        product=product.get("name"),
                        error=str(exc),
                    )

            logger.info(
                "category_done",
                source=source_key,
                category=category,
                total_records=len(records),
            )
            total_records += len(records)

            if not records:
                continue

            # ── Save locally ──
            local_path: Path | None = None
            if local_storage is not None:
                try:
                    local_path = local_storage.save(
                        records, source=source_key, category=category, run_date=run_date
                    )
                    logger.info("local_saved", path=str(local_path))
                except Exception as exc:
                    logger.error("local_save_failed", error=str(exc))

            # ── Upload to Azure ──
            if blob_uploader is not None and local_path is not None:
                try:
                    blob_name = blob_uploader.upload(
                        local_path, source=source_key, category=category, run_date=run_date
                    )
                    logger.info("blob_uploaded", blob=blob_name)
                except Exception as exc:
                    logger.error("blob_upload_failed", error=str(exc))

            # ── Legacy: direct Azure upload without local save ──
            if not use_local and not upload and not dry_run:
                try:
                    from storage.blob_uploader import BlobUploader
                    uploader = BlobUploader()
                    uploader.upload_records(
                        records, source=source_key, category=category, run_date=run_date
                    )
                except Exception as exc:
                    logger.error("storage_failed", error=str(exc))

    logger.info("pipeline_finished", total_records=total_records)


# ── Benchmark mode ─────────────────────────────────────────────────────────────

def run_benchmark(scraper: str | None, query: str, category: str) -> None:
    """Run the scraper benchmark and print a Rich report + save JSON."""
    from benchmark.runner import run_all
    from benchmark.report import render_console, render_summary, save_json

    logger.info("benchmark_mode", query=query, category=category, scraper=scraper or "all")
    metrics = run_all(category=category, query=query, scraper=scraper or None)
    render_console(metrics)
    render_summary(metrics)
    save_json(metrics)


# ── CLI ────────────────────────────────────────────────────────────────────────

@click.command()
@click.option(
    "--sources", multiple=True, default=None,
    help="Sources to scrape (default: all enabled sources from sources.yaml).",
)
@click.option(
    "--categories", multiple=True, default=None,
    help="Categories to scrape (default: all categories from products.yaml).",
)
@click.option("--dry-run", is_flag=True, default=False,
              help="Scrape and save locally but skip Azure upload.")
@click.option("--local", "use_local", is_flag=True, default=False,
              help="Save to local filesystem only (no Azure).")
@click.option("--upload", is_flag=True, default=False,
              help="Save locally then upload to Azure Blob Storage.")
@click.option(
    "--scraper", default=None,
    type=click.Choice(["bs4", "selenium", "playwright", "crawl4ai", "serpapi"]),
    help="Scraper approach to use (pipeline mode) or limit to (benchmark mode).",
)
@click.option("--benchmark", is_flag=True, default=False,
              help="Run full scraper benchmark comparison instead of pipeline.")
@click.option("--benchmark-query", default="MacBook Pro 14", show_default=True,
              help="Query string used in benchmark mode.")
@click.option("--benchmark-category", default="laptops", show_default=True,
              help="Category used in benchmark mode.")
def main(
    sources: tuple[str, ...],
    categories: tuple[str, ...],
    dry_run: bool,
    use_local: bool,
    upload: bool,
    scraper: str | None,
    benchmark: bool,
    benchmark_query: str,
    benchmark_category: str,
) -> None:
    if benchmark:
        run_benchmark(scraper=scraper, query=benchmark_query, category=benchmark_category)
        return

    sources_cfg: dict = load_yaml(settings.SOURCES_CONFIG)
    products_cfg: dict = load_yaml(settings.PRODUCTS_CONFIG)

    resolved_sources = (
        list(sources) if sources
        else [k for k, v in sources_cfg.items() if v.get("enabled")]
    )
    resolved_categories = list(categories) if categories else list(products_cfg.keys())

    run(
        sources=resolved_sources,
        categories=resolved_categories,
        dry_run=dry_run,
        use_local=use_local,
        upload=upload,
        approach=scraper,
    )


if __name__ == "__main__":
    main()
