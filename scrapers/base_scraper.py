"""
Abstract base class that every source-specific scraper must extend.

Contract
--------
Each concrete scraper must implement :meth:`search` which accepts a product
dict (from products.yaml) and returns a list of :class:`ProductRecord` dicts
matching the output schema defined in config/settings.py:

    product_name   str   – normalised product title from the listing
    price          float – numeric price (no currency symbol)
    currency       str   – ISO 4217 code, e.g. "EUR"
    availability   str   – "in_stock" | "out_of_stock" | "unknown"
    seller         str   – seller / merchant name if available, else source name
    source         str   – source key from sources.yaml, e.g. "amazon_de"
    url            str   – canonical product URL
    scrape_timestamp str – ISO-8601 UTC timestamp
    category       str   – "laptops" | "gpus" | "phones"
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import requests
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

ProductRecord = dict[str, Any]


class BaseScraper(ABC):
    """Base class for all price scrapers."""

    #: Override in subclasses with the key from sources.yaml
    source_key: str = ""

    def __init__(self, source_config: dict[str, Any]) -> None:
        self.cfg = source_config
        self.source_key = source_config.get("source_key", self.source_key)
        self.base_url: str = source_config["base_url"]
        self.currency: str = source_config.get("currency", "EUR")
        self.delay: float = float(source_config.get("rate_limit_delay", 2))
        self._ua = UserAgent()
        self._session = self._build_session()

    # ── Session ────────────────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self._ua.random})
        if settings.PROXIES:
            session.proxies.update(settings.PROXIES)
        return session

    def _random_ua_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self._ua.random,
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    # ── HTTP helpers ───────────────────────────────────────────────────────────

    @retry(stop=stop_after_attempt(settings.MAX_RETRIES), wait=wait_fixed(settings.RETRY_BACKOFF))
    def _get(self, url: str) -> requests.Response:
        """GET with retry logic and rotating User-Agent."""
        self._session.headers.update(self._random_ua_headers())
        response = self._session.get(url, timeout=settings.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response

    def _build_search_url(self, query: str) -> str:
        template: str = self.cfg["search_url_template"]
        return template.format(query=quote_plus(query))

    # ── Timestamp helper ───────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    # ── Rate limiting ──────────────────────────────────────────────────────────

    def _sleep(self) -> None:
        time.sleep(self.delay)

    # ── Abstract interface ─────────────────────────────────────────────────────

    @abstractmethod
    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        """
        Search for *product* and return a list of ProductRecord dicts.

        Parameters
        ----------
        product:
            A single product entry from products.yaml.
        category:
            The category key ("laptops", "gpus", or "phones").

        Returns
        -------
        list[ProductRecord]
            Zero or more scraped records matching the output schema.
        """

    # ── Record factory ─────────────────────────────────────────────────────────

    def _make_record(
        self,
        *,
        product_name: str,
        price: float,
        availability: str,
        seller: str,
        url: str,
        category: str,
        currency: str | None = None,
    ) -> ProductRecord:
        """Build a correctly-shaped output record."""
        return {
            "product_name": product_name,
            "price": price,
            "currency": currency or self.currency,
            "availability": availability,
            "seller": seller,
            "source": self.source_key,
            "url": url,
            "scrape_timestamp": self._now_iso(),
            "category": category,
        }
