"""
Crawl4AI + Groq LLM scrapers for Amazon DE, eBay DE, and Idealo DE.

Crawl4AI fetches and cleans the page (returns compact Markdown).
A Groq-hosted Llama model extracts structured product data from that
Markdown — no fragile CSS selectors, no HTML parsing.

Architecture
------------
- Each class bridges sync BaseScraper.search() → async _scrape_async() via
  asyncio.run().
- crawl4ai's AsyncWebCrawler + UndetectedAdapter (patchright under the hood)
  provides the best available bot-evasion without a residential proxy.
- Token usage and USD cost are tracked per call and logged.

LLM extraction
--------------
- System prompt: extraction-only instruction, JSON output.
- User prompt: page Markdown (capped at MAX_MD_CHARS to cap cost) + source hint.
- First parse failure triggers one retry with a stricter prompt.
- Groq llama-3.3-70b-versatile pricing: input $0.59/M tokens, output $0.79/M tokens.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
import warnings
from typing import Any

from groq import Groq
from bs4 import BeautifulSoup
from crawl4ai import AsyncLogger, AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from crawl4ai.browser_adapter import UndetectedAdapter

from config import settings
from scrapers.base_scraper import BaseScraper, ProductRecord
from utils.logger import get_logger

# Silence crawl4ai's requests-urllib3 version mismatch warning
warnings.filterwarnings("ignore", category=UserWarning, module="requests")

# crawl4ai's Rich logger needs UTF-8 to avoid cp1252 crashes on Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

logger = get_logger(__name__)

# ── LLM settings ───────────────────────────────────────────────────────────────

_MODEL = "llama-3.3-70b-versatile"
_INPUT_COST_PER_M  = 0.59   # USD per million input tokens  (Groq llama-3.3-70b-versatile)
_OUTPUT_COST_PER_M = 0.79   # USD per million output tokens (Groq llama-3.3-70b-versatile)
_MAX_MD_CHARS = 5_000       # ~3 k tokens/request; 3 sources × ~3.5 k ≈ 10.5 k < 12 k TPM limit

_SYSTEM_PROMPT = (
    "You are a data extraction assistant. Extract product listings from the "
    "provided page content. Return ONLY a valid JSON array with no markdown "
    "fences, no explanation, and no trailing text. "
    "Each element must have exactly these keys: "
    "product_name (string), price (number or null), currency (string, default 'EUR'), "
    "availability (string: 'in_stock' | 'out_of_stock' | 'unknown'), url (string or null)."
)

_RETRY_SYSTEM_PROMPT = (
    "You are a strict JSON generator. Output ONLY a raw JSON array. "
    "No markdown, no backticks, no explanation — just the array. "
    "Each element: {\"product_name\": str, \"price\": number|null, "
    "\"currency\": str, \"availability\": str, \"url\": str|null}."
)

# ── Price parser (kept for fallback normalisation) ─────────────────────────────

_PRICE_RE = re.compile(r"[\d.,]+")


def _parse_price(raw: Any) -> float:
    """Coerce LLM price output to float; return 0.0 on failure."""
    if isinstance(raw, (int, float)):
        return float(raw)
    if not raw:
        return 0.0
    text = str(raw).replace("\xa0", "").strip()
    m = _PRICE_RE.search(text)
    if not m:
        return 0.0
    token = m.group()
    if "," in token and "." in token:
        token = token.replace(".", "").replace(",", ".") if token.rfind(",") > token.rfind(".") else token.replace(",", "")
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
        return 0.0


# ── Cost tracking ──────────────────────────────────────────────────────────────

def _compute_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens * _INPUT_COST_PER_M + output_tokens * _OUTPUT_COST_PER_M) / 1_000_000


# ── Shared crawl4ai crawler factory ───────────────────────────────────────────

def _make_crawler_strategy() -> AsyncPlaywrightCrawlerStrategy:
    config = BrowserConfig(
        headless=True,
        verbose=False,
        viewport_width=1920,
        viewport_height=1080,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        headers={"Accept-Language": "de-DE,de;q=0.9,en;q=0.8"},
    )
    return AsyncPlaywrightCrawlerStrategy(
        browser_config=config,
        browser_adapter=UndetectedAdapter(),
        logger=AsyncLogger(verbose=False),
    )


# ── LLM extraction ─────────────────────────────────────────────────────────────

def _llm_extract(
    markdown: str,
    source_hint: str,
    client: Groq,
) -> tuple[list[dict], int, int]:
    """
    Call Groq Llama with the page markdown and return (items, input_tokens, output_tokens).
    Retries once with a stricter prompt if JSON parsing fails.
    """
    user_msg = (
        f"Source: {source_hint}\n\n"
        f"Page content (Markdown):\n{markdown[:_MAX_MD_CHARS]}"
    )

    def _call(system: str, user: str) -> tuple[str, int, int]:
        resp = client.chat.completions.create(
            model=_MODEL,
            max_tokens=4096,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        text = resp.choices[0].message.content if resp.choices else ""
        in_tok = resp.usage.prompt_tokens if resp.usage else 0
        out_tok = resp.usage.completion_tokens if resp.usage else 0
        return text, in_tok, out_tok

    # ── Attempt 1 ──
    raw, in_tok, out_tok = _call(_SYSTEM_PROMPT, user_msg)
    try:
        items = json.loads(raw.strip())
        if isinstance(items, list):
            return items, in_tok, out_tok
    except json.JSONDecodeError:
        logger.warning("llm_json_parse_fail_attempt1", source=source_hint, preview=raw[:200])

    # ── Attempt 2: stricter prompt + ask to fix ──
    retry_user = (
        f"{user_msg}\n\n"
        f"Your previous response was not valid JSON. Output ONLY the JSON array now."
    )
    raw2, in_tok2, out_tok2 = _call(_RETRY_SYSTEM_PROMPT, retry_user)
    # Strip any accidental markdown fences
    raw2 = re.sub(r"```(?:json)?|```", "", raw2).strip()
    try:
        items = json.loads(raw2)
        if isinstance(items, list):
            return items, in_tok + in_tok2, out_tok + out_tok2
    except json.JSONDecodeError:
        logger.error("llm_json_parse_fail_attempt2", source=source_hint, preview=raw2[:200])

    return [], in_tok + in_tok2, out_tok + out_tok2


# ── Smart markdown windowing ───────────────────────────────────────────────────

def _product_window(markdown: str) -> str:
    """
    Return a _MAX_MD_CHARS slice that contains actual product content.

    Product listing pages open with navigation / cookie banners (often 5–20 k
    chars of boilerplate before the first price).  We scan for the first price
    indicator ("€" or "EUR"), step back 200 chars for context, then take the
    next _MAX_MD_CHARS characters.  If no indicator is found we fall back to
    the very beginning.
    """
    for indicator in ("€", "EUR", " CHF", "Price"):
        pos = markdown.find(indicator)
        if pos != -1:
            start = max(0, pos - 200)
            return markdown[start : start + _MAX_MD_CHARS]
    return markdown[:_MAX_MD_CHARS]


# ── Base async crawl + extract logic ──────────────────────────────────────────

async def _crawl_and_extract(
    url: str,
    source_key: str,
    source_hint: str,
    product: dict[str, Any],
    category: str,
    scraper: BaseScraper,
    delay_s: float = 2.0,
    wait_for: str | None = None,
) -> list[ProductRecord]:
    """Fetch *url* with crawl4ai, send markdown to Haiku, build records."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        logger.error("groq_key_missing", note="Set GROQ_API_KEY in .env")
        return []

    client = Groq(api_key=api_key)
    strategy = _make_crawler_strategy()
    run_cfg = CrawlerRunConfig(
        verbose=False,
        delay_before_return_html=delay_s,
        **({"wait_for": wait_for} if wait_for else {}),
    )

    t0 = time.perf_counter()
    async with AsyncWebCrawler(
        crawler_strategy=strategy,
        logger=AsyncLogger(verbose=False),
    ) as crawler:
        result = await crawler.arun(url, config=run_cfg)

    fetch_s = time.perf_counter() - t0

    if not result.success:
        logger.error("crawl4ai_fetch_failed", source=source_key, url=url)
        return []

    html = result.html or ""
    markdown = (result.markdown.raw_markdown if result.markdown else "") or html[:_MAX_MD_CHARS]

    # DataDome detection
    if len(html) < 10_000 and "Something has gone wrong" in html:
        logger.warning(
            "crawl4ai_datadome_block",
            source=source_key,
            html_len=len(html),
            note=(
                "DataDome challenge page served even to patchright/UndetectedAdapter. "
                "A residential proxy is required to reach Idealo."
            ),
        )
        return []

    logger.info("crawl4ai_fetched", source=source_key, html_len=len(html),
                markdown_len=len(markdown), fetch_s=round(fetch_s, 2))

    # Smart markdown window: skip header/nav by finding where prices first appear
    md_slice = _product_window(markdown)

    # LLM extraction
    t1 = time.perf_counter()
    items, in_tok, out_tok = _llm_extract(md_slice, source_hint, client)
    llm_s = time.perf_counter() - t1
    cost_usd = _compute_cost(in_tok, out_tok)

    logger.info(
        "llm_extraction_done",
        source=source_key,
        items_found=len(items),
        input_tokens=in_tok,
        output_tokens=out_tok,
        cost_usd=round(cost_usd, 6),
        llm_s=round(llm_s, 2),
    )

    # Build ProductRecords
    records: list[ProductRecord] = []
    for item in items:
        name = str(item.get("product_name") or "").strip()
        if not name:
            continue
        url_val = str(item.get("url") or "").strip()
        avail_raw = str(item.get("availability") or "unknown").lower()
        availability = avail_raw if avail_raw in ("in_stock", "out_of_stock", "unknown") else "unknown"
        records.append(scraper._make_record(
            product_name=name,
            price=_parse_price(item.get("price")),
            currency=str(item.get("currency") or "EUR"),
            availability=availability,
            seller=source_key.replace("_", " ").title(),
            url=url_val,
            category=category,
        ))
    return records


# ── Amazon DE ──────────────────────────────────────────────────────────────────

class AmazonDECrawl4AIScraper(BaseScraper):
    """crawl4ai + Haiku scraper for Amazon.de."""

    source_key = "amazon_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records = await _crawl_and_extract(
            url=url,
            source_key=self.source_key,
            source_hint="Amazon.de product search results",
            product=product,
            category=category,
            scraper=self,
            delay_s=2.0,
        )
        self._sleep()
        return records


# ── eBay DE ────────────────────────────────────────────────────────────────────

class EbayDECrawl4AIScraper(BaseScraper):
    """crawl4ai + Haiku scraper for eBay.de."""

    source_key = "ebay_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records = await _crawl_and_extract(
            url=url,
            source_key=self.source_key,
            source_hint="eBay.de product search results",
            product=product,
            category=category,
            scraper=self,
            delay_s=2.0,
        )
        self._sleep()
        return records


# ── Idealo DE ──────────────────────────────────────────────────────────────────

class IdealoDECrawl4AIScraper(BaseScraper):
    """crawl4ai + Haiku scraper for Idealo.de.

    Uses UndetectedAdapter (patchright) for best available bot-detection
    evasion. DataDome still blocks at the TLS fingerprint level.
    """

    source_key = "idealo_de"

    def search(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        return asyncio.run(self._scrape_async(product, category))

    async def _scrape_async(self, product: dict[str, Any], category: str) -> list[ProductRecord]:
        query = product.get("search_query", product.get("name", ""))
        url = self._build_search_url(query)
        records = await _crawl_and_extract(
            url=url,
            source_key=self.source_key,
            source_hint="Idealo.de price comparison search results",
            product=product,
            category=category,
            scraper=self,
            delay_s=4.0,
        )
        self._sleep()
        return records
