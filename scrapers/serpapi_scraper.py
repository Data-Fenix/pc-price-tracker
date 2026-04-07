"""
SerpAPI scrapers for Google Shopping and Google organic (Idealo via site: filter).

Two scrapers are provided:

  SerpAPIGoogleShoppingScraper  — engine: "google_shopping", gl/hl: "de"
      Aggregates cross-retailer pricing in a single API call (1 credit).
      Returns up to ~20 listings including MediaMarkt, Saturn, Amazon, eBay,
      idealo.de, etc.

  SerpAPIIdealoDEScraper        — engine: "google", q: "site:idealo.de {query}"
      Circumvents Idealo's DataDome wall by reading Google's index instead of
      hitting idealo.de directly.  Each snippet usually contains the lowest
      listed price; the full Idealo comparison page URL is preserved.

Cost tracking
-------------
  SerpAPI pricing (as of 2026): $50 / 5 000 searches = $0.01 per search.
  Both scrapers log `credits_used` (always 1) and `cost_usd` (always 0.01).

Dry-run mode
------------
  Pass dry_run=True to load cached fixture JSON instead of hitting the API.
  Fixture files live in tests/fixtures/:
    serpapi_shopping_sample.json
    serpapi_organic_sample.json
  This costs $0 and is the default mode for the quick-test script.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from serpapi import GoogleSearch

from scrapers.base_scraper import BaseScraper, ProductRecord
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Pricing constants ──────────────────────────────────────────────────────────

_COST_PER_SEARCH = 0.01        # USD — $50 / 5 000 searches
_CREDITS_PER_SEARCH = 1

# ── Fixture paths (used in dry_run mode) ──────────────────────────────────────

_FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
_SHOPPING_FIXTURE = _FIXTURES_DIR / "serpapi_shopping_sample.json"
_ORGANIC_FIXTURE  = _FIXTURES_DIR / "serpapi_organic_sample.json"

# ── Price parser ───────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d.,]+")


def _parse_price(raw: Any) -> float:
    """Coerce a price string or number to float.  Returns 0.0 on failure."""
    if isinstance(raw, (int, float)):
        return float(raw)
    if not raw:
        return 0.0
    text = str(raw).replace("\xa0", "").strip()
    # Strip leading "ab " / "from " prefix common in Idealo snippets
    text = re.sub(r"(?i)^(ab|from)\s*", "", text)
    m = _PRICE_RE.search(text)
    if not m:
        return 0.0
    token = m.group()
    # German number format: "1.999,00" → 1999.00
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    elif "," in token:
        parts = token.split(",")
        token = token.replace(",", "") if (len(parts) == 2 and len(parts[-1]) == 3) else token.replace(",", ".")
    elif "." in token:
        parts = token.split(".")
        if len(parts) == 2 and len(parts[-1]) == 3:
            token = token.replace(".", "")
    try:
        return float(token)
    except ValueError:
        return 0.0


def _extract_price_from_snippet(snippet: str) -> float:
    """Pull the first EUR price out of an organic-result snippet."""
    # Match patterns like "1.999,00 €" or "ab 1.949,00 €"
    m = re.search(r"(?:ab\s*)?([\d.,]+)\s*(?:€|EUR)", snippet)
    if m:
        return _parse_price(m.group(1))
    return 0.0


# ── Shared default source config (BaseScraper requires base_url) ───────────────

def _default_cfg(source_key: str, base_url: str) -> dict:
    return {
        "source_key": source_key,
        "base_url": base_url,
        "currency": "EUR",
        "rate_limit_delay": 1,
    }


# ── Google Shopping scraper ────────────────────────────────────────────────────

class SerpAPIGoogleShoppingScraper(BaseScraper):
    """
    Searches Google Shopping (engine='google_shopping') via SerpAPI.

    One API call returns cross-retailer results for German locale.
    Cost: 1 credit = $0.01 per search() call.
    """

    source_key = "google_shopping"

    def __init__(
        self,
        source_config: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> None:
        super().__init__(source_config or _default_cfg(self.source_key, "https://serpapi.com"))
        self.dry_run = dry_run
        self._api_key = os.getenv("SERPAPI_KEY", "")
        if not self.dry_run and not self._api_key:
            logger.warning("serpapi_key_missing", note="Set SERPAPI_KEY in .env; using dry_run fallback")
            self.dry_run = True

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))

        if self.dry_run:
            logger.info("serpapi_shopping_dry_run", query=query, fixture=str(_SHOPPING_FIXTURE))
            raw = json.loads(_SHOPPING_FIXTURE.read_text(encoding="utf-8"))
        else:
            params = {
                "engine": "google_shopping",
                "q": query,
                "gl": "de",
                "hl": "de",
                "api_key": self._api_key,
            }
            logger.info("serpapi_shopping_live", query=query, credits=_CREDITS_PER_SEARCH)
            search = GoogleSearch(params)
            raw = search.get_dict()

        shopping_results: list[dict] = raw.get("shopping_results", [])
        records = self._parse_shopping_results(shopping_results, category)

        logger.info(
            "serpapi_shopping_done",
            query=query,
            items_found=len(records),
            credits_used=_CREDITS_PER_SEARCH,
            cost_usd=_COST_PER_SEARCH,
            dry_run=self.dry_run,
        )
        return records

    def _parse_shopping_results(
        self, results: list[dict], category: str
    ) -> list[ProductRecord]:
        records: list[ProductRecord] = []
        for item in results:
            name = str(item.get("title") or "").strip()
            if not name:
                continue

            # Prefer extracted_price (already a float) over raw price string
            price_raw = item.get("extracted_price") or item.get("price") or 0.0
            price = _parse_price(price_raw)

            seller = str(item.get("source") or "Google Shopping").strip()
            url    = str(item.get("link") or "").strip()
            avail  = "in_stock" if item.get("in_stock", True) and price > 0 else "unknown"

            records.append(self._make_record(
                product_name=name,
                price=price,
                availability=avail,
                seller=seller,
                url=url,
                category=category,
            ))
        return records


# ── Google organic → Idealo scraper ───────────────────────────────────────────

class SerpAPIIdealoDEScraper(BaseScraper):
    """
    Searches Google organic results filtered to idealo.de via SerpAPI.

    Query: "site:idealo.de {product_name}"  (engine='google', gl/hl='de')

    Bypasses Idealo's DataDome WAF by reading Google's index rather than
    hitting idealo.de directly.  Each organic result snippet typically contains
    the lowest listed price ("ab X,XX €").

    Cost: 1 credit = $0.01 per search() call.
    """

    source_key = "idealo_de_serpapi"

    def __init__(
        self,
        source_config: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> None:
        super().__init__(source_config or _default_cfg(self.source_key, "https://serpapi.com"))
        self.dry_run = dry_run
        self._api_key = os.getenv("SERPAPI_KEY", "")
        if not self.dry_run and not self._api_key:
            logger.warning("serpapi_key_missing", note="Set SERPAPI_KEY in .env; using dry_run fallback")
            self.dry_run = True

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = f"site:idealo.de {product.get('search_query', product.get('name', ''))}"

        if self.dry_run:
            logger.info("serpapi_organic_dry_run", query=query, fixture=str(_ORGANIC_FIXTURE))
            raw = json.loads(_ORGANIC_FIXTURE.read_text(encoding="utf-8"))
        else:
            params = {
                "engine": "google",
                "q": query,
                "gl": "de",
                "hl": "de",
                "api_key": self._api_key,
            }
            logger.info("serpapi_organic_live", query=query, credits=_CREDITS_PER_SEARCH)
            search = GoogleSearch(params)
            raw = search.get_dict()

        organic_results: list[dict] = raw.get("organic_results", [])
        records = self._parse_organic_results(organic_results, category)

        logger.info(
            "serpapi_organic_done",
            query=query,
            items_found=len(records),
            credits_used=_CREDITS_PER_SEARCH,
            cost_usd=_COST_PER_SEARCH,
            dry_run=self.dry_run,
        )
        return records

    def _parse_organic_results(
        self, results: list[dict], category: str
    ) -> list[ProductRecord]:
        records: list[ProductRecord] = []
        for item in results:
            title   = str(item.get("title") or "").strip()
            snippet = str(item.get("snippet") or "").strip()
            url     = str(item.get("link") or "").strip()

            if not title or "idealo" not in url.lower():
                continue

            # Clean Idealo title suffixes like " | Preisvergleich bei idealo"
            name = re.sub(r"\s*\|.*$", "", title).strip()
            if not name:
                continue

            # Try snippet for price ("ab 1.999,00 €" or "1.999,00 € kaufen")
            price = _extract_price_from_snippet(snippet)

            avail = "in_stock" if price > 0 else "unknown"

            records.append(self._make_record(
                product_name=name,
                price=price,
                availability=avail,
                seller="Idealo DE (via Google)",
                url=url,
                category=category,
            ))
        return records
