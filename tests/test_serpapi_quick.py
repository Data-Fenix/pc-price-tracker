"""
Quick smoke-test for SerpAPI scrapers.

Searches "MacBook Pro 14" via:
  1. Google Shopping  (SerpAPIGoogleShoppingScraper)
  2. Google organic → idealo.de  (SerpAPIIdealoDEScraper)

Modes
-----
  Dry run (default, 0 credits):
      python tests/test_serpapi_quick.py

  Live run (2 credits = $0.02):
      python tests/test_serpapi_quick.py --live
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

from tabulate import tabulate

from scrapers.serpapi_scraper import (
    SerpAPIGoogleShoppingScraper,
    SerpAPIIdealoDEScraper,
)

PRODUCT  = {"name": "MacBook Pro 14", "search_query": "MacBook Pro 14"}
CATEGORY = "laptops"

# $0.01 per search, 1 credit per search
_COST_PER_SEARCH = 0.01


def run_scraper(scraper, label: str) -> tuple[list, float, float]:
    t0 = time.perf_counter()
    records = scraper.search(PRODUCT, CATEGORY)
    elapsed = time.perf_counter() - t0
    cost = _COST_PER_SEARCH if not scraper.dry_run else 0.0
    print(f"\n-- {label} --")
    if not records:
        print("  No results.")
    else:
        preview = []
        for r in records[:3]:
            preview.append({
                "product_name": r["product_name"][:70],
                "price":        r["price"],
                "currency":     r["currency"],
                "availability": r["availability"],
                "seller":       r["seller"],
                "url":          r["url"][:70],
            })
        print(json.dumps(preview, indent=2, ensure_ascii=False))
    return records, elapsed, cost


def main() -> None:
    live = "--live" in sys.argv
    dry_run = not live

    mode_label = "LIVE (credits will be consumed)" if live else "DRY RUN (fixture data, no credits)"
    print(f"\n{'=' * 70}")
    print(f'  SerpAPI Scraper Quick Test  query: "{PRODUCT["search_query"]}"')
    print(f"  Mode: {mode_label}")
    print(f"{'=' * 70}")

    shopping_scraper = SerpAPIGoogleShoppingScraper(dry_run=dry_run)
    idealo_scraper   = SerpAPIIdealoDEScraper(dry_run=dry_run)

    summary_rows = []

    # ── Google Shopping ──
    records_shop, t_shop, cost_shop = run_scraper(
        shopping_scraper, "GOOGLE SHOPPING"
    )
    summary_rows.append([
        "Google Shopping",
        len(records_shop),
        f"{t_shop:.2f}s",
        f"{'$0.01' if live else '$0.00 (dry)'}",
        "1" if live else "0",
    ])

    # ── Idealo via Google organic ──
    records_idea, t_idea, cost_idea = run_scraper(
        idealo_scraper, "IDEALO DE (via Google organic)"
    )
    summary_rows.append([
        "Idealo via Google",
        len(records_idea),
        f"{t_idea:.2f}s",
        f"{'$0.01' if live else '$0.00 (dry)'}",
        "1" if live else "0",
    ])

    total_cost = cost_shop + cost_idea

    print(f"\n{'=' * 70}")
    print("  Summary")
    print("=" * 70)
    print(tabulate(
        summary_rows,
        headers=["Source", "Products found", "Time", "Cost", "Credits used"],
        tablefmt="github",
    ))
    print(f"\n  Total cost this run: ${total_cost:.2f}  |  Credits used: {int(live) * 2}")

    # ── Idealo coverage analysis ──
    idealo_in_shopping = [
        r for r in records_shop
        if "idealo" in r.get("seller", "").lower() or "idealo" in r.get("url", "").lower()
    ]
    print(f"\n  Idealo listings appearing in Google Shopping: {len(idealo_in_shopping)}")
    for r in idealo_in_shopping:
        print(f"    - {r['product_name'][:60]}  @  €{r['price']:.2f}  ({r['seller']})")

    print()


if __name__ == "__main__":
    main()
