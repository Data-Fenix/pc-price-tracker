"""
Benchmark runner — times every scraper approach against every source.

Usage
-----
    from benchmark.runner import run_all
    metrics = run_all(category="laptops", query="MacBook Pro 14")

    # Single approach (faster iteration):
    metrics = run_all(scraper="serpapi")

CLI flag (via run_pipeline.py):
    python run_pipeline.py --benchmark [--scraper bs4|selenium|playwright|crawl4ai|serpapi]

Concurrency model
-----------------
Each scraper *approach* runs in its own thread (ThreadPoolExecutor, max 5
workers).  Within each approach the three sources are run sequentially so
we never spin up more than one Chrome instance per approach at once.
SerpAPI and BS4 are I/O-bound HTTP; Selenium / Playwright / Crawl4AI each
manage their own browser process per call and are thread-safe as long as
no two calls share a driver / page object.
"""
from __future__ import annotations

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

import yaml

from benchmark.metrics import ScraperMetrics
from utils.logger import get_logger

logger = get_logger(__name__)

_SOURCES_YAML = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"

# ── Scraper-approach cost constants (for approaches that call paid APIs) ───────

_SERPAPI_COST   = 0.01      # USD per search
_CRAWL4AI_COST  = 0.002     # USD estimate per call (Groq llama-3.3-70b, ~3.5k tokens)
_CRAWL4AI_TOKENS = 3_500    # input + output estimate

# ── Source config loader ───────────────────────────────────────────────────────

def _load_sources() -> dict:
    with _SOURCES_YAML.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _source_cfg(sources: dict, key: str) -> dict:
    cfg = dict(sources[key])
    cfg["source_key"] = key
    return cfg


# ── Per-approach runner helpers ────────────────────────────────────────────────

def _run_one(
    scraper_name: str,
    source: str,
    category: str,
    product: dict[str, Any],
    factory: Callable,
    cost_usd: float,
    tokens_used: int,
) -> ScraperMetrics:
    """Instantiate and call one scraper, return a populated ScraperMetrics."""
    t0 = time.perf_counter()
    try:
        scraper = factory()
        records = scraper.search(product, category)
        elapsed = time.perf_counter() - t0
        logger.info(
            "benchmark_run_done",
            scraper=scraper_name,
            source=source,
            products=len(records),
            time_s=round(elapsed, 2),
        )
        return ScraperMetrics(
            scraper_name=scraper_name,
            source=source,
            category=category,
            products_found=len(records),
            time_seconds=round(elapsed, 2),
            success=True,
            error_message="",
            cost_usd=cost_usd,
            tokens_used=tokens_used,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        msg = f"{type(exc).__name__}: {exc}"
        logger.error(
            "benchmark_run_failed",
            scraper=scraper_name,
            source=source,
            error=msg,
            time_s=round(elapsed, 2),
        )
        return ScraperMetrics(
            scraper_name=scraper_name,
            source=source,
            category=category,
            products_found=0,
            time_seconds=round(elapsed, 2),
            success=False,
            error_message=msg,
            cost_usd=0.0,
            tokens_used=0,
        )


# ── Approach groups: each returns a list[ScraperMetrics] ──────────────────────

def _run_beautifulsoup(product: dict, category: str, sources: dict) -> list[ScraperMetrics]:
    from scrapers.beautifulsoup_scraper import (
        AmazonDEScraper,
        EbayDEScraper,
        IdealoDEScraper,
    )
    pairs = [
        ("amazon_de", lambda: AmazonDEScraper(_source_cfg(sources, "amazon_de"))),
        ("ebay_de",   lambda: EbayDEScraper(_source_cfg(sources, "ebay_de"))),
        ("idealo_de", lambda: IdealoDEScraper(_source_cfg(sources, "idealo_de"))),
    ]
    return [
        _run_one("beautifulsoup", src, category, product, factory, 0.0, 0)
        for src, factory in pairs
    ]


def _run_selenium(product: dict, category: str, sources: dict) -> list[ScraperMetrics]:
    from scrapers.selenium_scraper import (
        AmazonDESeleniumScraper,
        EbayDESeleniumScraper,
        IdealoDESeleniumScraper,
    )
    pairs = [
        ("amazon_de", lambda: AmazonDESeleniumScraper(_source_cfg(sources, "amazon_de"))),
        ("ebay_de",   lambda: EbayDESeleniumScraper(_source_cfg(sources, "ebay_de"))),
        ("idealo_de", lambda: IdealoDESeleniumScraper(_source_cfg(sources, "idealo_de"))),
    ]
    return [
        _run_one("selenium", src, category, product, factory, 0.0, 0)
        for src, factory in pairs
    ]


def _run_playwright(product: dict, category: str, sources: dict) -> list[ScraperMetrics]:
    from scrapers.playwright_scraper import (
        AmazonDEPlaywrightScraper,
        EbayDEPlaywrightScraper,
        IdealoDEPlaywrightScraper,
    )
    pairs = [
        ("amazon_de", lambda: AmazonDEPlaywrightScraper(_source_cfg(sources, "amazon_de"))),
        ("ebay_de",   lambda: EbayDEPlaywrightScraper(_source_cfg(sources, "ebay_de"))),
        ("idealo_de", lambda: IdealoDEPlaywrightScraper(_source_cfg(sources, "idealo_de"))),
    ]
    return [
        _run_one("playwright", src, category, product, factory, 0.0, 0)
        for src, factory in pairs
    ]


def _run_crawl4ai(product: dict, category: str, sources: dict) -> list[ScraperMetrics]:
    from scrapers.crawl4ai_scraper import (
        AmazonDECrawl4AIScraper,
        EbayDECrawl4AIScraper,
        IdealoDECrawl4AIScraper,
    )
    pairs = [
        ("amazon_de", lambda: AmazonDECrawl4AIScraper(_source_cfg(sources, "amazon_de"))),
        ("ebay_de",   lambda: EbayDECrawl4AIScraper(_source_cfg(sources, "ebay_de"))),
        ("idealo_de", lambda: IdealoDECrawl4AIScraper(_source_cfg(sources, "idealo_de"))),
    ]
    return [
        _run_one("crawl4ai", src, category, product, factory, _CRAWL4AI_COST, _CRAWL4AI_TOKENS)
        for src, factory in pairs
    ]


def _run_serpapi(product: dict, category: str, _sources: dict) -> list[ScraperMetrics]:
    from scrapers.serpapi_scraper import (
        SerpAPIGoogleShoppingScraper,
        SerpAPIIdealoDEScraper,
    )
    pairs = [
        ("google_shopping",    lambda: SerpAPIGoogleShoppingScraper(dry_run=False)),
        ("idealo_de_serpapi",  lambda: SerpAPIIdealoDEScraper(dry_run=False)),
    ]
    return [
        _run_one("serpapi", src, category, product, factory, _SERPAPI_COST, 0)
        for src, factory in pairs
    ]


# ── Approach registry ──────────────────────────────────────────────────────────

_APPROACHES: dict[str, Callable] = {
    "bs4":        _run_beautifulsoup,
    "selenium":   _run_selenium,
    "playwright": _run_playwright,
    "crawl4ai":   _run_crawl4ai,
    "serpapi":    _run_serpapi,
}

# ── Public entry point ─────────────────────────────────────────────────────────

def run_all(
    category: str = "laptops",
    query: str = "MacBook Pro 14",
    scraper: str | None = None,
) -> list[ScraperMetrics]:
    """
    Run benchmark across all (or one) scraper approach(es).

    Parameters
    ----------
    category:
        Product category key ("laptops", "gpus", "phones").
    query:
        Search query string passed to every scraper.
    scraper:
        If given, run only this approach key ("bs4", "selenium",
        "playwright", "crawl4ai", "serpapi").  None → all five.

    Returns
    -------
    list[ScraperMetrics]
        One entry per (approach, source) combination attempted.
    """
    product = {"name": query, "search_query": query}
    sources = _load_sources()

    if scraper is not None:
        if scraper not in _APPROACHES:
            raise ValueError(f"Unknown scraper '{scraper}'. Choose from: {list(_APPROACHES)}")
        approaches = {scraper: _APPROACHES[scraper]}
    else:
        approaches = _APPROACHES

    logger.info(
        "benchmark_started",
        query=query,
        category=category,
        approaches=list(approaches),
    )

    all_metrics: list[ScraperMetrics] = []

    # Each approach runs in its own thread; sources within an approach are sequential
    with ThreadPoolExecutor(max_workers=len(approaches)) as pool:
        futures = {
            pool.submit(fn, product, category, sources): name
            for name, fn in approaches.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                metrics_batch = future.result()
                all_metrics.extend(metrics_batch)
            except Exception as exc:
                logger.error("benchmark_approach_crashed", approach=name, error=str(exc))

    all_metrics.sort(key=lambda m: (m.scraper_name, m.source))
    logger.info("benchmark_finished", total_runs=len(all_metrics))
    return all_metrics
