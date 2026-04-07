"""
Benchmark runner — orchestrates timed scrape runs and collects raw metrics.

Usage (planned)
---------------
    from benchmark.runner import BenchmarkRunner
    runner = BenchmarkRunner(sources=["amazon_de"], categories=["laptops"])
    results = runner.run()

TODO
----
- Accept a list of (source, category) pairs to benchmark.
- For each pair: instantiate the scraper, time the scrape, capture records.
- Delegate metric calculation to benchmark.metrics.
- Delegate report generation to benchmark.report.
"""
from __future__ import annotations

from typing import Any

from utils.logger import get_logger

logger = get_logger(__name__)


class BenchmarkRunner:
    """Orchestrate benchmark runs across scraper/category combinations."""

    def __init__(
        self,
        sources: list[str] | None = None,
        categories: list[str] | None = None,
    ) -> None:
        self.sources = sources or []
        self.categories = categories or []

    def run(self) -> list[dict[str, Any]]:
        """
        Execute benchmark runs and return raw result records.

        TODO
        ----
        - Iterate over sources × categories.
        - Time each scrape with time.perf_counter.
        - Collect: duration_seconds, record_count, error_count, source, category.
        - Return list of result dicts for use by MetricsCalculator.
        """
        raise NotImplementedError("BenchmarkRunner.run not yet implemented")
