"""
BeautifulSoup scrapers for Amazon DE, eBay DE, and Idealo DE.

Each class inherits BaseScraper and implements search() using
requests + BeautifulSoup with the lxml parser.

Retry logic (3 attempts, exponential backoff) is provided by the
overridden _get() here; rate-limiting via self._sleep() after each
search page.
"""
from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from scrapers.base_scraper import BaseScraper, ProductRecord
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Price helpers ──────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d.,]+")


def _parse_price(raw: str) -> float | None:
    """
    Convert a German-formatted price string to float.

    Handles:
      "1.299,99 €"  →  1299.99
      "1.299,99"    →  1299.99
      "299,99"      →  299.99
      "299.99"      →  299.99   (dot-decimal, no thousands)
    """
    raw = raw.replace("\xa0", " ").strip()
    m = _PRICE_RE.search(raw)
    if not m:
        return None
    token = m.group()

    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            # German: "1.299,99"
            token = token.replace(".", "").replace(",", ".")
        else:
            # English: "1,299.99"
            token = token.replace(",", "")
    elif "," in token:
        parts = token.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            token = token.replace(",", "")       # "1,299" → thousands
        else:
            token = token.replace(",", ".")       # "299,99" → decimal
    elif "." in token:
        parts = token.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            token = token.replace(".", "")        # "1.299" → thousands

    try:
        return float(token)
    except ValueError:
        return None


# ── Shared _get override ───────────────────────────────────────────────────────

def _make_get(multiplier: int = 1, min_wait: int = 2, max_wait: int = 30):
    """Return a tenacity-decorated _get method with exponential backoff."""
    def _get(self, url: str):
        self._session.headers.update(self._random_ua_headers())
        response = self._session.get(url, timeout=settings.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response
    return retry(
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
        reraise=True,
    )(_get)


# ── Amazon DE ──────────────────────────────────────────────────────────────────

class AmazonDEScraper(BaseScraper):
    """Scrape Amazon.de search result pages with BeautifulSoup.

    URL strategy: Amazon wraps all search-result links in /sspa/click tracking
    URLs.  We use the container's data-asin attribute to build a canonical
    /dp/{asin} product URL instead.
    """

    source_key = "amazon_de"
    _get = _make_get(multiplier=1, min_wait=2, max_wait=30)  # type: ignore[assignment]

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []

        try:
            response = self._get(url)
            soup = BeautifulSoup(response.text, "lxml")
            containers = soup.select('div[data-component-type="s-search-result"]')
            logger.info("amazon_containers_found", count=len(containers), query=query)

            for item in containers:
                # ── Name ──
                name_el = item.select_one("h2 span")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue

                # ── Price: offscreen span has the full formatted price ──
                price: float | None = None
                offscreen = item.select_one("span.a-price span.a-offscreen")
                if offscreen:
                    price = _parse_price(offscreen.get_text(strip=True))
                if price is None:
                    whole_el = item.select_one("span.a-price-whole")
                    frac_el = item.select_one("span.a-price-fraction")
                    if whole_el:
                        whole = re.sub(r"[,.]$", "", whole_el.get_text(strip=True))
                        frac = frac_el.get_text(strip=True) if frac_el else "00"
                        price = _parse_price(f"{whole},{frac}")

                # ── URL: build canonical /dp/{asin} from data-asin ──
                asin = item.get("data-asin", "")
                if asin:
                    href = f"https://www.amazon.de/dp/{asin}"
                else:
                    # Fallback: first non-sspa /dp/ link
                    href = ""
                    for a in item.select("a[href]"):
                        candidate = a.get("href", "")
                        if "/dp/" in candidate and not candidate.startswith("/sspa"):
                            href = "https://www.amazon.de" + candidate if candidate.startswith("/") else candidate
                            break
                    if not href:
                        continue

                # ── Availability ──
                availability = "in_stock" if price else "out_of_stock"

                # ── Seller ──
                seller_el = (
                    item.select_one("span.s-size-mini.s-color-base")
                    or item.select_one("span.a-size-base.s-underline-text")
                )
                seller = seller_el.get_text(strip=True) if seller_el else "Amazon DE"

                records.append(
                    self._make_record(
                        product_name=name,
                        price=price or 0.0,
                        availability=availability,
                        seller=seller,
                        url=href,
                        category=category,
                    )
                )
        except Exception as exc:
            logger.error("amazon_search_failed", error=str(exc), product=product.get("name"))

        self._sleep()
        return records


# ── eBay DE ────────────────────────────────────────────────────────────────────

class EbayDEScraper(BaseScraper):
    """Scrape eBay.de search result pages with BeautifulSoup.

    eBay DE now uses li.s-card containers (not the old li.s-item).
    Cards without an ebay.de/itm/ link are US cross-border "Shop on eBay"
    placeholder cards and are skipped.
    """

    source_key = "ebay_de"
    _get = _make_get(multiplier=1, min_wait=2, max_wait=30)  # type: ignore[assignment]

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []

        try:
            response = self._get(url)
            soup = BeautifulSoup(response.text, "lxml")
            all_cards = soup.select("li.s-card")
            # Filter to cards that have a real ebay.de listing link
            containers = [
                c for c in all_cards
                if any("ebay.de/itm/" in a.get("href", "") for a in c.select("a[href]"))
            ]
            logger.info("ebay_containers_found", count=len(containers), query=query)

            for item in containers:
                # ── Name: first primary-styled text in the title div ──
                name_el = item.select_one("div.s-card__title span.su-styled-text")
                if not name_el:
                    name_el = item.select_one("div.s-card__title")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name.lower().startswith("shop on ebay"):
                    continue

                # ── Price: first attribute row (price row) ──
                price: float | None = None
                price_row = item.select_one("div.s-card__attribute-row")
                if price_row:
                    price_text = price_row.get_text(strip=True)
                    if "EUR" in price_text or "€" in price_text:
                        price = _parse_price(price_text)

                # ── URL: first ebay.de/itm/ link ──
                href = ""
                for a in item.select("a[href]"):
                    candidate = a.get("href", "")
                    if "ebay.de/itm/" in candidate:
                        href = candidate
                        break
                if not href:
                    continue

                # ── Availability ──
                availability = "in_stock" if price else "out_of_stock"

                # ── Seller: secondary attribute section, first primary span ──
                seller = "eBay DE"
                sec_attrs = item.select_one("div.su-card-container__attributes__secondary")
                if sec_attrs:
                    seller_el = sec_attrs.select_one("span.su-styled-text")
                    if seller_el:
                        seller = seller_el.get_text(strip=True)

                records.append(
                    self._make_record(
                        product_name=name,
                        price=price or 0.0,
                        availability=availability,
                        seller=seller,
                        url=href,
                        category=category,
                    )
                )
        except Exception as exc:
            logger.error("ebay_search_failed", error=str(exc), product=product.get("name"))

        self._sleep()
        return records


# ── Idealo DE ──────────────────────────────────────────────────────────────────

class IdealoDEScraper(BaseScraper):
    """
    Scrape Idealo.de search result pages with BeautifulSoup.

    Note: Idealo uses heavy JS rendering; the HTML returned by a plain
    requests GET is a server-side rendered skeleton.  We attempt to parse
    what SSR delivers and fall back gracefully when JS-gated elements are
    absent.  A 503 means Idealo's WAF blocked the request — retry headers
    are rotated automatically.
    """

    source_key = "idealo_de"
    _get = _make_get(multiplier=2, min_wait=3, max_wait=60)  # type: ignore[assignment]

    # Extra headers Idealo needs to serve SSR content
    _IDEALO_HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "de-DE,de;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Upgrade-Insecure-Requests": "1",
    }

    def _get(self, url: str):  # type: ignore[override]
        self._session.headers.update(self._random_ua_headers())
        self._session.headers.update(self._IDEALO_HEADERS)
        response = self._session.get(url, timeout=settings.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response

    _get = retry(  # type: ignore[assignment]
        stop=stop_after_attempt(settings.MAX_RETRIES),
        wait=wait_exponential(multiplier=2, min=3, max=60),
        reraise=True,
    )(_get)

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []

        try:
            response = self._get(url)
            soup = BeautifulSoup(response.text, "lxml")

            # Idealo frequently changes class names — try several known patterns
            containers = (
                soup.select("div.productOffers-listItem")
                or soup.select("div[class*='sr-resultList'] > div[class*='sr-result']")
                or soup.select("article[class*='offer']")
                or soup.select("div[data-test='product-tile']")
                or soup.select("div[class*='ProductTile']")
                or soup.select("div[class*='product-tile']")
            )
            logger.info("idealo_containers_found", count=len(containers), query=query)

            for item in containers:
                # ── Name ──
                name_el = (
                    item.select_one("a.offerList-item-name-inner")
                    or item.select_one("a[class*='product-name']")
                    or item.select_one("[class*='productName']")
                    or item.select_one("h2")
                    or item.select_one("h3")
                )
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue

                # ── Price ──
                price_el = (
                    item.select_one("span.price-amount")
                    or item.select_one("[class*='idealPrice']")
                    or item.select_one("[class*='price']")
                )
                price: float | None = None
                if price_el:
                    price = _parse_price(price_el.get_text(strip=True))

                # ── URL ──
                link_el = (
                    item.select_one("a.offerList-item-name-inner")
                    or item.select_one("a[class*='product-name']")
                    or item.select_one("a[href]")
                )
                href = ""
                if link_el:
                    href = link_el.get("href", "")
                    if href.startswith("/"):
                        href = "https://www.idealo.de" + href

                availability = "in_stock" if price else "unknown"

                seller_el = item.select_one("[class*='seller']") or item.select_one("[class*='shop']")
                seller = seller_el.get_text(strip=True) if seller_el else "Idealo DE"

                if name:
                    records.append(
                        self._make_record(
                            product_name=name,
                            price=price or 0.0,
                            availability=availability,
                            seller=seller,
                            url=href,
                            category=category,
                        )
                    )
        except Exception as exc:
            msg = str(exc)
            if "503" in msg or "Service Unavailable" in msg:
                logger.warning(
                    "idealo_waf_block",
                    product=product.get("name"),
                    note="Idealo WAF blocks plain requests. Use Playwright scraper instead.",
                )
            else:
                logger.error("idealo_search_failed", error=msg, product=product.get("name"))

        self._sleep()
        return records
