"""
Playwright scrapers for Amazon DE, eBay DE, and Idealo DE.

Uses async Playwright with playwright-stealth to bypass bot-detection walls
(specifically Idealo's DataDome challenge) that block requests and Selenium.

Architecture
------------
- Each concrete class exposes the synchronous BaseScraper.search() interface.
- search() calls asyncio.run(_scrape_async()) to bridge into async Playwright.
- The browser is always closed in a finally block to prevent zombie processes.

Stealth setup (Stealth class, playwright-stealth 2.0.3)
--------------------------------------------------------
- navigator.webdriver masked
- navigator.plugins / languages / platform spoofed (de-DE, Win32)
- chrome.runtime injected
- sec-ch-ua header spoofed
- Applied via Stealth.apply_stealth_async(page) before any navigation.

Idealo strategy (three escalating attempts)
-------------------------------------------
1. Direct search URL + 3 s settle + mouse movements.
2. Homepage-first two-step navigation + 2 s settle + mouse movements.
3. If DataDome is still detected after both, log definitive verdict.
"""
from __future__ import annotations

import asyncio
import random
import re
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PWTimeout,
    async_playwright,
)
from playwright_stealth import Stealth

from scrapers.base_scraper import BaseScraper, ProductRecord
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
_SEC_CH_UA = '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'

_WAIT_MS = 15_000       # explicit-wait timeout in ms
_SETTLE_S = 3           # post-load settle before scraping
_IDEALO_SETTLE_S = 4    # extra settle for Idealo


# ── Shared stealth instance ────────────────────────────────────────────────────

_STEALTH = Stealth(
    navigator_languages_override=("de-DE", "de"),
    navigator_platform_override="Win32",
    navigator_user_agent_override=_UA,
    sec_ch_ua_override=_SEC_CH_UA,
    webgl_vendor_override="Intel Inc.",
    webgl_renderer_override="Intel Iris OpenGL Engine",
)


# ── Price parser ───────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d.,]+")


def _parse_price(raw: str) -> float | None:
    raw = raw.replace("\xa0", " ").strip()
    m = _PRICE_RE.search(raw)
    if not m:
        return None
    token = m.group()
    if "," in token and "." in token:
        token = (
            token.replace(".", "").replace(",", ".")
            if token.rfind(",") > token.rfind(".")
            else token.replace(",", "")
        )
    elif "," in token:
        parts = token.split(",")
        token = token.replace(",", "") if (len(parts) == 2 and len(parts[1]) == 3) else token.replace(",", ".")
    elif "." in token:
        parts = token.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            token = token.replace(".", "")
    try:
        return float(token)
    except ValueError:
        return None


# ── Browser / context factory ──────────────────────────────────────────────────

async def _new_context(browser: Browser) -> BrowserContext:
    """Create a browser context with realistic headers and stealth applied."""
    ctx = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent=_UA,
        locale="de-DE",
        extra_http_headers={
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": _SEC_CH_UA,
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        },
    )
    # Mask webdriver at the context level (before any page opens)
    await ctx.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return ctx


async def _new_page(ctx: BrowserContext) -> Page:
    """Open a new page and apply stealth evasions."""
    page = await ctx.new_page()
    await _STEALTH.apply_stealth_async(page)
    return page


async def _human_mouse(page: Page, steps: int = 4) -> None:
    """Move mouse to several random viewport coordinates."""
    for _ in range(steps):
        x = random.randint(200, 1700)
        y = random.randint(100, 900)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.3))


# ── DataDome detection ─────────────────────────────────────────────────────────

def _is_datadome(page_source: str) -> bool:
    return len(page_source) < 10_000 and "Something has gone wrong" in page_source


# ── BS4 extraction helpers ─────────────────────────────────────────────────────

def _extract_amazon(soup: BeautifulSoup, category: str, scraper: BaseScraper) -> list[ProductRecord]:
    records: list[ProductRecord] = []
    for item in soup.select('div[data-component-type="s-search-result"]'):
        name_el = item.select_one("h2 span")
        if not name_el or not (name := name_el.get_text(strip=True)):
            continue
        price: float | None = None
        off = item.select_one("span.a-price span.a-offscreen")
        if off:
            price = _parse_price(off.get_text(strip=True))
        if price is None:
            w = item.select_one("span.a-price-whole")
            f = item.select_one("span.a-price-fraction")
            if w:
                price = _parse_price(f"{re.sub(r'[,.]$', '', w.get_text(strip=True))},{f.get_text(strip=True) if f else '00'}")
        asin = item.get("data-asin", "")
        href = f"https://www.amazon.de/dp/{asin}" if asin else ""
        if not href:
            continue
        seller_el = item.select_one("span.s-size-mini.s-color-base") or item.select_one("span.a-size-base.s-underline-text")
        records.append(scraper._make_record(
            product_name=name, price=price or 0.0,
            availability="in_stock" if price else "out_of_stock",
            seller=seller_el.get_text(strip=True) if seller_el else "Amazon DE",
            url=href, category=category,
        ))
    return records


def _extract_ebay(soup: BeautifulSoup, category: str, scraper: BaseScraper) -> list[ProductRecord]:
    records: list[ProductRecord] = []
    for item in soup.select("li.s-card"):
        if not any("ebay.de/itm/" in a.get("href", "") for a in item.select("a[href]")):
            continue
        name_el = item.select_one("div.s-card__title span.su-styled-text") or item.select_one("div.s-card__title")
        if not name_el or not (name := name_el.get_text(strip=True)):
            continue
        if name.lower().startswith("shop on ebay"):
            continue
        price: float | None = None
        row = item.select_one("div.s-card__attribute-row")
        if row:
            rt = row.get_text(strip=True)
            if "EUR" in rt or "€" in rt:
                price = _parse_price(rt)
        href = next((a.get("href", "") for a in item.select("a[href]") if "ebay.de/itm/" in a.get("href", "")), "")
        if not href:
            continue
        seller = "eBay DE"
        sec = item.select_one("div.su-card-container__attributes__secondary")
        if sec and (sel := sec.select_one("span.su-styled-text")):
            seller = sel.get_text(strip=True)
        records.append(scraper._make_record(
            product_name=name, price=price or 0.0,
            availability="in_stock" if price else "out_of_stock",
            seller=seller, url=href, category=category,
        ))
    return records


_IDEALO_CONTAINERS = [
    "div.productOffers-listItem",
    "div[class*='sr-resultItem']",
    "div[class*='ResultItem']",
    "article[class*='offer']",
    "div[data-test='product-tile']",
    "div[class*='ProductTile']",
    "div[class*='product-tile']",
    "section[class*='offer']",
]


def _extract_idealo(soup: BeautifulSoup, category: str, scraper: BaseScraper) -> list[ProductRecord]:
    records: list[ProductRecord] = []
    containers: list = []
    for sel in _IDEALO_CONTAINERS:
        found = soup.select(sel)
        if found:
            containers = found
            break
    for item in containers:
        name_el = (
            item.select_one("a.offerList-item-name-inner")
            or item.select_one("a[class*='product-name']")
            or item.select_one("[class*='productName']")
            or item.select_one("[class*='ProductName']")
            or item.select_one("h2") or item.select_one("h3")
        )
        if not name_el or not (name := name_el.get_text(strip=True)):
            continue
        price_el = (
            item.select_one("span.price-amount")
            or item.select_one("[class*='idealPrice']")
            or item.select_one("[class*='price']")
            or item.select_one("[class*='Price']")
        )
        price = _parse_price(price_el.get_text(strip=True)) if price_el else None
        link_el = item.select_one("a.offerList-item-name-inner") or item.select_one("a[class*='product-name']") or item.select_one("a[href]")
        href = ""
        if link_el:
            href = link_el.get("href", "")
            if href.startswith("/"):
                href = "https://www.idealo.de" + href
        seller_el = item.select_one("[class*='seller']") or item.select_one("[class*='shop']")
        records.append(scraper._make_record(
            product_name=name, price=price or 0.0,
            availability="in_stock" if price else "unknown",
            seller=seller_el.get_text(strip=True) if seller_el else "Idealo DE",
            url=href, category=category,
        ))
    return records


# ── Amazon DE ──────────────────────────────────────────────────────────────────

class AmazonDEPlaywrightScraper(BaseScraper):
    source_key = "amazon_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                ctx = await _new_context(browser)
                page = await _new_page(ctx)
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                try:
                    await page.wait_for_selector(
                        'div[data-component-type="s-search-result"]',
                        timeout=_WAIT_MS,
                    )
                except PWTimeout:
                    logger.warning("amazon_pw_wait_timeout", query=query)

                soup = BeautifulSoup(await page.content(), "lxml")
                records = _extract_amazon(soup, category, self)
                logger.info("amazon_pw_records", count=len(records), query=query)
            except Exception as exc:
                logger.error("amazon_pw_failed", error=str(exc), product=product.get("name"))
            finally:
                await browser.close()

        self._sleep()
        return records


# ── eBay DE ────────────────────────────────────────────────────────────────────

class EbayDEPlaywrightScraper(BaseScraper):
    source_key = "ebay_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                ctx = await _new_context(browser)
                page = await _new_page(ctx)
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                try:
                    await page.wait_for_selector("li.s-card", timeout=_WAIT_MS)
                except PWTimeout:
                    logger.warning("ebay_pw_wait_timeout", query=query)

                soup = BeautifulSoup(await page.content(), "lxml")
                records = _extract_ebay(soup, category, self)
                logger.info("ebay_pw_records", count=len(records), query=query)
            except Exception as exc:
                logger.error("ebay_pw_failed", error=str(exc), product=product.get("name"))
            finally:
                await browser.close()

        self._sleep()
        return records


# ── Idealo DE ──────────────────────────────────────────────────────────────────

class IdealoDEPlaywrightScraper(BaseScraper):
    """Playwright scraper for Idealo.de with multi-stage DataDome bypass.

    Attempts (in order):
      1. Direct navigation to search URL + settle + mouse movements.
      2. Two-step: homepage → search URL + longer settle + mouse movements.
    If both fail, logs a definitive message.
    """

    source_key = "idealo_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        search_url = self._build_search_url(query)

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                # ── Attempt 1: direct navigation ────────────────────────────
                records = await self._attempt_direct(browser, search_url, query, product, category)
                if records is not None:
                    return records

                # ── Attempt 2: homepage-first two-step ──────────────────────
                records = await self._attempt_two_step(browser, search_url, query, product, category)
                if records is not None:
                    return records

                logger.warning(
                    "idealo_pw_datadome_unbypassable",
                    product=product.get("name"),
                    note=(
                        "DataDome blocks Playwright headless Chromium even with stealth. "
                        "A residential proxy or undetected-chromium build is required."
                    ),
                )
                return []
            finally:
                await browser.close()

    # ── Attempt helpers ────────────────────────────────────────────────────────

    async def _make_page(self, browser: Browser) -> tuple[BrowserContext, Page]:
        ctx = await _new_context(browser)
        page = await _new_page(ctx)
        return ctx, page

    async def _settle_and_move(self, page: Page, settle: float = _IDEALO_SETTLE_S) -> None:
        await asyncio.sleep(settle)
        await page.evaluate("window.scrollTo(0, 600)")
        await asyncio.sleep(0.5)
        await _human_mouse(page, steps=5)
        await asyncio.sleep(random.uniform(0.5, 1.0))
        await page.evaluate("window.scrollTo(0, 1400)")
        await asyncio.sleep(0.5)

    async def _wait_for_products(self, page: Page) -> bool:
        """Wait for any known Idealo product container. Returns True if found."""
        combined = ", ".join(_IDEALO_CONTAINERS)
        try:
            await page.wait_for_selector(combined, timeout=_WAIT_MS)
            return True
        except PWTimeout:
            return False

    async def _parse_page(
        self,
        page: Page,
        query: str,
        product: dict,
        category: str,
        attempt: int,
    ) -> list[ProductRecord] | None:
        """
        Return a list of records if the page is valid, None if DataDome was hit.
        """
        src = await page.content()
        if _is_datadome(src):
            logger.warning(
                "idealo_pw_datadome",
                attempt=attempt,
                query=query,
                product=product.get("name"),
            )
            return None  # signal caller to try next strategy

        soup = BeautifulSoup(src, "lxml")
        records = _extract_idealo(soup, category, self)
        logger.info("idealo_pw_records", attempt=attempt, count=len(records), query=query)

        if not records:
            # Log a snippet to help diagnose selector drift
            body_text = soup.get_text()[:600].replace("\n", " ")
            logger.warning("idealo_pw_no_products", attempt=attempt, snippet=body_text[:300])

        return records  # may be empty list, but not None

    async def _attempt_direct(
        self,
        browser: Browser,
        search_url: str,
        query: str,
        product: dict,
        category: str,
    ) -> list[ProductRecord] | None:
        """Attempt 1: go straight to search URL."""
        ctx, page = await self._make_page(browser)
        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
            await self._settle_and_move(page)
            await self._wait_for_products(page)
            return await self._parse_page(page, query, product, category, attempt=1)
        except Exception as exc:
            logger.error("idealo_pw_attempt1_error", error=str(exc))
            return None
        finally:
            await ctx.close()

    async def _attempt_two_step(
        self,
        browser: Browser,
        search_url: str,
        query: str,
        product: dict,
        category: str,
    ) -> list[ProductRecord] | None:
        """Attempt 2: visit homepage first, then navigate to search URL."""
        ctx, page = await self._make_page(browser)
        try:
            # Step A — homepage warm-up
            await page.goto("https://www.idealo.de", wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(2)
            await _human_mouse(page, steps=3)

            # Step B — navigate to search
            await page.goto(search_url, wait_until="domcontentloaded", timeout=30_000)
            await self._settle_and_move(page, settle=_IDEALO_SETTLE_S + 1)
            await self._wait_for_products(page)
            return await self._parse_page(page, query, product, category, attempt=2)
        except Exception as exc:
            logger.error("idealo_pw_attempt2_error", error=str(exc))
            return None
        finally:
            await ctx.close()
