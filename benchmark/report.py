"""
Report generator — formats benchmark metrics for human consumption.

Planned output formats
-----------------------
- Console table  (via tabulate)
- JSON file      (machine-readable, suitable for CI artefacts)
- Markdown file  (suitable for GitHub PR comments)

TODO
----
- Accept MetricsSummary dict from MetricsCalculator.compute().
- render_console() → print a formatted table to stdout.
- render_json(path) → write JSON to file.
- render_markdown(path) → write Markdown summary to file.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


class ReportGenerator:
    """Turn a MetricsSummary into human-readable output."""

    def __init__(self, summary: dict[str, Any]) -> None:
        self.summary = summary

    def render_console(self) -> None:
        """Print a formatted table to stdout using tabulate.

        TODO: implement using tabulate with 'github' table format.
        """
        raise NotImplementedError("ReportGenerator.render_console not yet implemented")

    def render_json(self, path: Path | str) -> None:
        """Write the summary as indented JSON to *path*.

        TODO: implement with json.dumps(indent=2).
        """
        raise NotImplementedError("ReportGenerator.render_json not yet implemented")

    def render_markdown(self, path: Path | str) -> None:
        """Write a Markdown summary table to *path*.

        TODO: implement with tabulate 'pipe' format wrapped in Markdown headers.
        """
        raise NotImplementedError("ReportGenerator.render_markdown not yet implemented")
