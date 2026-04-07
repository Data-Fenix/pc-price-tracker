"""
ScraperMetrics dataclass — one record per (scraper_approach, source, category) run.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class ScraperMetrics:
    """Captures all measurable dimensions of a single scraper run."""

    scraper_name: str      # "beautifulsoup" | "selenium" | "playwright" | "crawl4ai" | "serpapi"
    source: str            # "amazon_de" | "ebay_de" | "idealo_de" | "google_shopping" | …
    category: str          # "laptops" | "gpus" | "phones"
    products_found: int    # number of ProductRecords returned
    time_seconds: float    # wall-clock seconds for the search() call
    success: bool          # True when no exception was raised
    error_message: str     # "" on success, exception text on failure
    cost_usd: float        # 0.0 for free scrapers; API cost for LLM / SerpAPI
    tokens_used: int       # 0 for non-LLM scrapers; input+output tokens for Crawl4AI
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── Derived metric ─────────────────────────────────────────────────────────

    def cost_per_product(self) -> float:
        """USD cost divided by products found; 0.0 when no products returned."""
        if self.products_found == 0:
            return 0.0
        return round(self.cost_usd / self.products_found, 6)

    # ── Serialisation ──────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        d = asdict(self)
        d["cost_per_product"] = self.cost_per_product()
        return d
