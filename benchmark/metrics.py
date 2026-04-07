"""
Metrics calculator for benchmark results.

Planned metrics
---------------
- scrape_duration_seconds   – wall-clock time per (source, category) run
- records_scraped            – number of ProductRecords returned
- success_rate               – records_scraped / products_attempted
- price_fill_rate            – fraction of records where price > 0
- missing_fields_rate        – fraction of records with any null required field

TODO
----
- Accept raw result list from BenchmarkRunner.run().
- Compute aggregate stats (mean, min, max, p95) for duration.
- Return a structured MetricsSummary dict for use by ReportGenerator.
"""
from __future__ import annotations

from typing import Any


class MetricsCalculator:
    """Compute quality and performance metrics from raw benchmark results."""

    def __init__(self, raw_results: list[dict[str, Any]]) -> None:
        self.raw = raw_results

    def compute(self) -> dict[str, Any]:
        """
        Return a MetricsSummary dict.

        TODO
        ----
        - Group raw_results by (source, category).
        - For each group compute per-source metrics.
        - Compute global aggregates.
        """
        raise NotImplementedError("MetricsCalculator.compute not yet implemented")
