"""
Selenium scrapers for Amazon DE, eBay DE, and Idealo DE.

Uses headless Chrome via webdriver-manager (auto-downloads ChromeDriver).
Key differences from BeautifulSoup scraper:
  - Each search() call creates a fresh WebDriver and destroys it in finally.
  - Explicit WebDriverWait replaces time.sleep wherever possible.
  - The real browser executes JavaScript, making Idealo's JS-rendered content
    accessible without Playwright.
  - Page source is handed to BeautifulSoup for parsing (same selectors reused).
"""
from __future__ import annotations

import re
import time
from typing import Any

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import OperationSystemManager  # kept for reference

from scrapers.base_scraper import BaseScraper, ProductRecord
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

# A stable Chrome 124 UA — recognised by most sites, avoids headless detection
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_WAIT_TIMEOUT = 15      # seconds for explicit waits
_POST_LOAD_PAUSE = 2    # seconds after DOMContentLoaded before scraping Idealo


# ── Price parser (identical to beautifulsoup_scraper) ──────────────────────────

_PRICE_RE = re.compile(r"[\d.,]+")


def _parse_price(raw: str) -> float | None:
    raw = raw.replace("\xa0", " ").strip()
    m = _PRICE_RE.search(raw)
    if not m:
        return None
    token = m.group()
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
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


# ── Driver factory ─────────────────────────────────────────────────────────────

def _build_driver() -> webdriver.Chrome:
    """Create a headless Chrome WebDriver with anti-detection hardening.

    Uses Selenium Manager (built into Selenium 4.6+) to auto-download the
    matching ChromeDriver — more reliable than webdriver-manager for Chrome
    115+ (Chrome-for-Testing distribution) and correct on win64.
    No explicit Service is needed; Selenium Manager runs transparently.
    """
    opts = Options()
    opts.add_argument("--headless=new")          # new headless mode (Chrome >= 112)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=de-DE")
    opts.add_argument(f"--user-agent={_USER_AGENT}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # No Service() → Selenium Manager downloads the correct win64 ChromeDriver
    driver = webdriver.Chrome(options=opts)

    # Mask webdriver property to reduce bot-detection fingerprint
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    driver.execute_cdp_cmd(
        "Network.setExtraHTTPHeaders",
        {"headers": {"Accept-Language": "de-DE,de;q=0.9,en;q=0.8"}},
    )
    return driver


# ── Amazon DE ──────────────────────────────────────────────────────────────────

class AmazonDESeleniumScraper(BaseScraper):
    """Selenium-based scraper for Amazon.de search results.

    Waits for the first s-search-result container before parsing.
    Uses the same data-asin → /dp/{asin} URL strategy as the BS4 scraper.
    """

    source_key = "amazon_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []
        driver = _build_driver()

        try:
            driver.get(url)

            # Wait until at least one result container is present
            try:
                WebDriverWait(driver, _WAIT_TIMEOUT).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'div[data-component-type="s-search-result"]')
                    )
                )
            except TimeoutException:
                logger.warning("amazon_wait_timeout", query=query)

            soup = BeautifulSoup(driver.page_source, "lxml")
            containers = soup.select('div[data-component-type="s-search-result"]')
            logger.info("amazon_selenium_containers", count=len(containers), query=query)

            for item in containers:
                # Name
                name_el = item.select_one("h2 span")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue

                # Price
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

                # URL — data-asin is most reliable
                asin = item.get("data-asin", "")
                if asin:
                    href = f"https://www.amazon.de/dp/{asin}"
                else:
                    href = ""
                    for a in item.select("a[href]"):
                        candidate = a.get("href", "")
                        if "/dp/" in candidate and not candidate.startswith("/sspa"):
                            href = ("https://www.amazon.de" + candidate
                                    if candidate.startswith("/") else candidate)
                            break
                    if not href:
                        continue

                availability = "in_stock" if price else "out_of_stock"
                seller_el = (
                    item.select_one("span.s-size-mini.s-color-base")
                    or item.select_one("span.a-size-base.s-underline-text")
                )
                seller = seller_el.get_text(strip=True) if seller_el else "Amazon DE"

                records.append(self._make_record(
                    product_name=name,
                    price=price or 0.0,
                    availability=availability,
                    seller=seller,
                    url=href,
                    category=category,
                ))

        except WebDriverException as exc:
            logger.error("amazon_selenium_driver_error", error=str(exc), product=product.get("name"))
        except Exception as exc:
            logger.error("amazon_selenium_failed", error=str(exc), product=product.get("name"))
        finally:
            driver.quit()

        self._sleep()
        return records


# ── eBay DE ────────────────────────────────────────────────────────────────────

class EbayDESeleniumScraper(BaseScraper):
    """Selenium-based scraper for eBay.de search results.

    Waits for li.s-card containers; filters out US cross-border cards.
    """

    source_key = "ebay_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []
        driver = _build_driver()

        try:
            driver.get(url)

            try:
                WebDriverWait(driver, _WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "li.s-card"))
                )
            except TimeoutException:
                logger.warning("ebay_wait_timeout", query=query)

            soup = BeautifulSoup(driver.page_source, "lxml")
            all_cards = soup.select("li.s-card")
            containers = [
                c for c in all_cards
                if any("ebay.de/itm/" in a.get("href", "") for a in c.select("a[href]"))
            ]
            logger.info("ebay_selenium_containers", count=len(containers), query=query)

            for item in containers:
                # Name
                name_el = item.select_one("div.s-card__title span.su-styled-text")
                if not name_el:
                    name_el = item.select_one("div.s-card__title")
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name or name.lower().startswith("shop on ebay"):
                    continue

                # Price — first attribute row containing EUR/€
                price: float | None = None
                price_row = item.select_one("div.s-card__attribute-row")
                if price_row:
                    price_text = price_row.get_text(strip=True)
                    if "EUR" in price_text or "€" in price_text:
                        price = _parse_price(price_text)

                # URL
                href = ""
                for a in item.select("a[href]"):
                    candidate = a.get("href", "")
                    if "ebay.de/itm/" in candidate:
                        href = candidate
                        break
                if not href:
                    continue

                availability = "in_stock" if price else "out_of_stock"
                seller = "eBay DE"
                sec_attrs = item.select_one("div.su-card-container__attributes__secondary")
                if sec_attrs:
                    seller_el = sec_attrs.select_one("span.su-styled-text")
                    if seller_el:
                        seller = seller_el.get_text(strip=True)

                records.append(self._make_record(
                    product_name=name,
                    price=price or 0.0,
                    availability=availability,
                    seller=seller,
                    url=href,
                    category=category,
                ))

        except WebDriverException as exc:
            logger.error("ebay_selenium_driver_error", error=str(exc), product=product.get("name"))
        except Exception as exc:
            logger.error("ebay_selenium_failed", error=str(exc), product=product.get("name"))
        finally:
            driver.quit()

        self._sleep()
        return records


# ── Idealo DE ──────────────────────────────────────────────────────────────────

class IdealoDESeleniumScraper(BaseScraper):
    """Selenium-based scraper for Idealo.de.

    Idealo is heavily JS-rendered and blocks plain requests (503).
    A real headless Chrome browser passes the initial WAF check and executes
    the page JS, making product data available in the DOM.

    Strategy:
      1. Load the search URL.
      2. Wait _POST_LOAD_PAUSE seconds after DOMContentLoaded.
      3. Scroll down 600 px to trigger lazy-loaded content.
      4. Wait for any known product container selector.
      5. Hand page_source to BeautifulSoup for extraction.
    """

    source_key = "idealo_de"

    # Selector cascade — Idealo renames classes frequently
    _CONTAINER_SELECTORS = [
        "div.productOffers-listItem",
        "div[class*='sr-resultItem']",
        "div[class*='ResultItem']",
        "article[class*='offer']",
        "div[data-test='product-tile']",
        "div[class*='ProductTile']",
        "div[class*='product-tile']",
        "section[class*='offer']",
    ]

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records: list[ProductRecord] = []
        driver = _build_driver()

        try:
            driver.get(url)

            # Let the page settle after initial load
            time.sleep(_POST_LOAD_PAUSE)

            # Scroll to trigger lazy-loaded content / pass scroll-depth checks
            driver.execute_script("window.scrollTo(0, 500)")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 1200)")
            time.sleep(1)

            # Early-exit on DataDome bot-protection page
            if self._is_datadome_block(driver.page_source):
                logger.warning(
                    "idealo_datadome_block",
                    product=product.get("name"),
                    note=(
                        "Idealo serves a DataDome JS-challenge page to headless Chrome. "
                        "Use the Playwright scraper (scrapers/playwright_scraper.py) "
                        "with playwright-stealth to bypass it."
                    ),
                )
                return records

            # Try each known container selector; wait for the first that appears
            container_sel = None
            for sel in self._CONTAINER_SELECTORS:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    container_sel = sel
                    logger.info("idealo_container_found", selector=sel, query=query)
                    break
                except TimeoutException:
                    continue

            if container_sel is None:
                # Log a snippet of the page to help diagnose selector drift
                snippet = driver.page_source[3000:5000].replace("\n", " ")
                logger.warning(
                    "idealo_no_container",
                    query=query,
                    page_snippet=snippet[:400],
                )

            soup = BeautifulSoup(driver.page_source, "lxml")

            containers: list = []
            if container_sel:
                containers = soup.select(container_sel)
            else:
                # Last-resort: try all candidates on the rendered page
                for sel in self._CONTAINER_SELECTORS:
                    found = soup.select(sel)
                    if found:
                        containers = found
                        break

            logger.info("idealo_selenium_containers", count=len(containers), query=query)

            for item in containers:
                # Name
                name_el = (
                    item.select_one("a.offerList-item-name-inner")
                    or item.select_one("a[class*='product-name']")
                    or item.select_one("[class*='productName']")
                    or item.select_one("[class*='ProductName']")
                    or item.select_one("h2")
                    or item.select_one("h3")
                )
                if not name_el:
                    continue
                name = name_el.get_text(strip=True)
                if not name:
                    continue

                # Price
                price_el = (
                    item.select_one("span.price-amount")
                    or item.select_one("[class*='idealPrice']")
                    or item.select_one("[class*='price']")
                    or item.select_one("[class*='Price']")
                )
                price: float | None = None
                if price_el:
                    price = _parse_price(price_el.get_text(strip=True))

                # URL
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
                seller_el = (
                    item.select_one("[class*='seller']")
                    or item.select_one("[class*='shop']")
                )
                seller = seller_el.get_text(strip=True) if seller_el else "Idealo DE"

                if name:
                    records.append(self._make_record(
                        product_name=name,
                        price=price or 0.0,
                        availability=availability,
                        seller=seller,
                        url=href,
                        category=category,
                    ))

        except WebDriverException as exc:
            logger.error("idealo_selenium_driver_error", error=str(exc), product=product.get("name"))
        except Exception as exc:
            logger.error("idealo_selenium_failed", error=str(exc), product=product.get("name"))
        finally:
            driver.quit()

        self._sleep()
        return records

    @staticmethod
    def _is_datadome_block(page_source: str) -> bool:
        """Return True when Idealo serves a DataDome challenge page."""
        return (
            len(page_source) < 10_000
            and "Something has gone wrong" in page_source
        )
