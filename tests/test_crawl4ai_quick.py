"""
Quick smoke-test for Crawl4AI + LLM scrapers.

Searches "MacBook Pro 14" across all three sources, prints the first
3 results per source as JSON, and a timing/cost summary table.

Run:
    python tests/test_crawl4ai_quick.py
"""
import io
import json
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml
from tabulate import tabulate

from scrapers.crawl4ai_scraper import (
    AmazonDECrawl4AIScraper,
    EbayDECrawl4AIScraper,
    IdealoDECrawl4AIScraper,
)

SOURCES_CFG = ROOT / "config" / "sources.yaml"
PRODUCT = {"name": "MacBook Pro 14", "search_query": "MacBook Pro 14"}
CATEGORY = "laptops"

SCRAPERS = {
    "amazon_de": AmazonDECrawl4AIScraper,
    "ebay_de":   EbayDECrawl4AIScraper,
    "idealo_de": IdealoDECrawl4AIScraper,
}


def load_source_cfg(source_key: str) -> dict:
    with SOURCES_CFG.open("r", encoding="utf-8") as fh:
        all_cfg = yaml.safe_load(fh)
    cfg = all_cfg[source_key]
    cfg["source_key"] = source_key
    return cfg


def run_source(source_key: str, scraper_cls) -> tuple[list, float]:
    cfg = load_source_cfg(source_key)
    scraper = scraper_cls(cfg)
    t0 = time.perf_counter()
    records = scraper.search(PRODUCT, CATEGORY)
    elapsed = time.perf_counter() - t0
    return records, elapsed


def main() -> None:
    # Capture log output to parse token/cost metrics
    import logging
    import structlog

    # Collect structlog events so we can surface cost data in the table
    captured_events: list[dict] = []

    original_factory = structlog.get_config().get("logger_factory")

    summary_rows = []
    print(f"\n{'=' * 70}")
    print(f'  Crawl4AI + LLM Scraper Quick Test  query: "{PRODUCT["search_query"]}"')
    print(f"{'=' * 70}\n")

    for source_key, scraper_cls in SCRAPERS.items():
        print(f"-- {source_key.upper()} ------------------------------------------")
        try:
            records, elapsed = run_source(source_key, scraper_cls)
        except Exception as exc:
            print(f"  ERROR: {exc}\n")
            summary_rows.append([source_key, "ERROR", "0.00s", "-", "-"])
            continue

        if not records:
            print("  No results returned.\n")
        else:
            # Show first 3 records
            preview = []
            for r in records[:3]:
                preview.append({
                    "product_name": r.get("product_name") if isinstance(r, dict) else getattr(r, "product_name", ""),
                    "price":        r.get("price")        if isinstance(r, dict) else getattr(r, "price", None),
                    "currency":     r.get("currency")     if isinstance(r, dict) else getattr(r, "currency", "EUR"),
                    "availability": r.get("availability") if isinstance(r, dict) else getattr(r, "availability", ""),
                    "seller":       r.get("seller")       if isinstance(r, dict) else getattr(r, "seller", ""),
                    "url":          (r.get("url") if isinstance(r, dict) else getattr(r, "url", ""))[:80],
                })
            print(json.dumps(preview, indent=2, ensure_ascii=False))
            print()

        # Token/cost data is in structlog output; we cannot easily intercept
        # it post-hoc without wrapping, so we note N/A and rely on console logs.
        summary_rows.append([source_key, len(records), f"{elapsed:.2f}s", "see logs", "see logs"])

    print(f"\n{'=' * 70}")
    print("  Summary")
    print("=" * 70)
    print(tabulate(
        summary_rows,
        headers=["Source", "Products found", "Time", "Tokens (in+out)", "Cost USD"],
        tablefmt="github",
    ))
    print()
    print("  Token usage and cost per call are logged inline above (llm_extraction_done events).")
    print()


if __name__ == "__main__":
    main()
