"""
PC & Electronics Price Tracker — pipeline entry point.

Usage
-----
    # Run all enabled sources and categories
    python run_pipeline.py

    # Restrict to specific sources / categories
    python run_pipeline.py --sources amazon_de ebay_de --categories laptops gpus

    # Dry-run (scrape but skip upload)
    python run_pipeline.py --dry-run

    # Use local storage instead of Azure Blob
    python run_pipeline.py --local
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


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_scraper(source_key: str, source_cfg: dict[str, Any]):
    """Dynamically import and instantiate the scraper class for *source_key*."""
    class_name: str = source_cfg["scraper_class"]
    module_name = f"scrapers.{source_key}"
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
    except (ModuleNotFoundError, AttributeError) as exc:
        logger.error("scraper_load_failed", source=source_key, error=str(exc))
        raise
    source_cfg["source_key"] = source_key
    return cls(source_cfg)


def run(
    sources: list[str],
    categories: list[str],
    dry_run: bool,
    use_local: bool,
) -> None:
    sources_cfg: dict = load_yaml(settings.SOURCES_CONFIG)
    products_cfg: dict = load_yaml(settings.PRODUCTS_CONFIG)
    run_date: date = datetime.now(timezone.utc).date()

    logger.info(
        "pipeline_started",
        run_date=str(run_date),
        sources=sources,
        categories=categories,
        dry_run=dry_run,
    )

    # Resolve storage backend
    if not dry_run:
        if use_local:
            from storage.local_storage import LocalStorage
            storage = LocalStorage()
        else:
            from storage.blob_uploader import BlobUploader
            storage = BlobUploader()

    total_records = 0

    for source_key in sources:
        if source_key not in sources_cfg:
            logger.warning("unknown_source", source=source_key)
            continue

        source_cfg = sources_cfg[source_key]
        if not source_cfg.get("enabled", True):
            logger.info("source_disabled", source=source_key)
            continue

        try:
            scraper = load_scraper(source_key, source_cfg)
        except Exception:
            logger.warning("skipping_source", source=source_key)
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

            if not dry_run and records:
                try:
                    dest = storage.save(records, source=source_key, category=category, run_date=run_date) \
                        if use_local else \
                        storage.upload(records, source=source_key, category=category, run_date=run_date)
                    logger.info("records_stored", destination=str(dest))
                except Exception as exc:
                    logger.error("storage_failed", error=str(exc))

    logger.info("pipeline_finished", total_records=total_records)


@click.command()
@click.option(
    "--sources",
    multiple=True,
    default=None,
    help="Sources to scrape (default: all enabled sources from sources.yaml).",
)
@click.option(
    "--categories",
    multiple=True,
    default=None,
    help="Categories to scrape (default: all categories from products.yaml).",
)
@click.option("--dry-run", is_flag=True, default=False, help="Scrape but skip storage.")
@click.option("--local", "use_local", is_flag=True, default=False, help="Use local filesystem storage.")
def main(
    sources: tuple[str, ...],
    categories: tuple[str, ...],
    dry_run: bool,
    use_local: bool,
) -> None:
    sources_cfg: dict = load_yaml(settings.SOURCES_CONFIG)
    products_cfg: dict = load_yaml(settings.PRODUCTS_CONFIG)

    resolved_sources = list(sources) if sources else [k for k, v in sources_cfg.items() if v.get("enabled")]
    resolved_categories = list(categories) if categories else list(products_cfg.keys())

    run(
        sources=resolved_sources,
        categories=resolved_categories,
        dry_run=dry_run,
        use_local=use_local,
    )


if __name__ == "__main__":
    main()
