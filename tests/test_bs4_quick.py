"""
Quick smoke-test for BeautifulSoup scrapers.

Searches for "MacBook Pro 14" across all three sources, prints the
first 3 results per source as JSON, and a timing summary table.

Run:
    python tests/test_bs4_quick.py
"""
import io
import json
import sys
import time
from pathlib import Path

# Force UTF-8 output on Windows so box-drawing / emoji chars don't crash
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Ensure project root is on sys.path when run directly
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yaml
from tabulate import tabulate

from scrapers.beautifulsoup_scraper import AmazonDEScraper, EbayDEScraper, IdealoDEScraper

SOURCES_CFG = ROOT / "config" / "sources.yaml"

PRODUCT = {"name": "MacBook Pro 14", "search_query": "MacBook Pro 14"}
CATEGORY = "laptops"

SCRAPERS = {
    "amazon_de": AmazonDEScraper,
    "ebay_de": EbayDEScraper,
    "idealo_de": IdealoDEScraper,
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
    summary_rows = []
    print(f"\n{'=' * 60}")
    print(f"  BS4 Scraper Quick Test  query: \"{PRODUCT['search_query']}\"")
    print(f"{'=' * 60}\n")

    for source_key, scraper_cls in SCRAPERS.items():
        print(f"-- {source_key.upper()} ------------------------------------------")
        try:
            records, elapsed = run_source(source_key, scraper_cls)
        except Exception as exc:
            print(f"  ERROR: {exc}\n")
            summary_rows.append([source_key, "ERROR", f"{0:.2f}s"])
            continue

        if not records:
            print("  No results returned.\n")
        else:
            sample = records[:3]
            print(json.dumps(sample, indent=2, ensure_ascii=False))
            print()

        summary_rows.append([source_key, len(records), f"{elapsed:.2f}s"])

    print(f"\n{'=' * 60}")
    print("  Summary")
    print('=' * 60)
    print(tabulate(summary_rows, headers=["Source", "Products found", "Time"], tablefmt="github"))
    print()


if __name__ == "__main__":
    main()
