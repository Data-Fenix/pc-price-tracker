"""
Report generator — formats ScraperMetrics into console tables and JSON files.

Outputs
-------
1. Rich console table  — per-run breakdown (scraper, source, products, time, cost, status)
2. Summary stats table — fastest / most products / cheapest per source + overall success rates
3. JSON file           — benchmark/results/{YYYY-MM-DD}.json (machine-readable, article source)
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table
from rich import box

from benchmark.metrics import ScraperMetrics

_RESULTS_DIR = Path(__file__).resolve().parent / "results"
_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

_console = Console()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _status_str(m: ScraperMetrics) -> str:
    if not m.success:
        short = m.error_message[:40] + "…" if len(m.error_message) > 40 else m.error_message
        return f"[red]FAIL[/red] {short}"
    if m.products_found == 0:
        return "[yellow]OK / 0 results[/yellow]"
    return f"[green]OK[/green]"


def _fmt_cost(cost: float) -> str:
    if cost == 0.0:
        return "[dim]$0.00[/dim]"
    return f"${cost:.4f}"


def _fmt_cpp(cpp: float) -> str:
    if cpp == 0.0:
        return "[dim]-[/dim]"
    return f"${cpp:.5f}"


# ── Main table ────────────────────────────────────────────────────────────────

def render_console(metrics: list[ScraperMetrics]) -> None:
    """Print the per-run Rich table to stdout."""
    table = Table(
        title="Scraper Benchmark Results",
        box=box.MARKDOWN,
        show_lines=False,
        header_style="bold cyan",
    )
    table.add_column("Scraper",       style="bold")
    table.add_column("Source",        style="dim")
    table.add_column("Products",      justify="right")
    table.add_column("Time (s)",      justify="right")
    table.add_column("Cost",          justify="right")
    table.add_column("Cost/Product",  justify="right")
    table.add_column("Tokens",        justify="right")
    table.add_column("Status")

    for m in sorted(metrics, key=lambda x: (x.scraper_name, x.source)):
        table.add_row(
            m.scraper_name,
            m.source,
            str(m.products_found),
            f"{m.time_seconds:.1f}",
            _fmt_cost(m.cost_usd),
            _fmt_cpp(m.cost_per_product()),
            str(m.tokens_used) if m.tokens_used else "[dim]-[/dim]",
            _status_str(m),
        )

    _console.print()
    _console.print(table)


# ── Summary stats table ───────────────────────────────────────────────────────

def render_summary(metrics: list[ScraperMetrics]) -> None:
    """Print per-source winner stats and overall success rates."""

    # ── Per-source stats ──
    by_source: dict[str, list[ScraperMetrics]] = defaultdict(list)
    for m in metrics:
        by_source[m.source].append(m)

    source_table = Table(
        title="Per-Source Winners",
        box=box.MARKDOWN,
        header_style="bold magenta",
    )
    source_table.add_column("Source")
    source_table.add_column("Fastest scraper")
    source_table.add_column("Most products")
    source_table.add_column("Cheapest per product")

    for source, rows in sorted(by_source.items()):
        successful = [r for r in rows if r.success]
        if not successful:
            source_table.add_row(source, "[red]all failed[/red]", "-", "-")
            continue

        fastest = min(successful, key=lambda r: r.time_seconds)
        most    = max(successful, key=lambda r: r.products_found)
        with_products = [r for r in successful if r.products_found > 0]
        cheapest = min(with_products, key=lambda r: r.cost_per_product()) if with_products else None

        source_table.add_row(
            source,
            f"{fastest.scraper_name} ({fastest.time_seconds:.1f}s)",
            f"{most.scraper_name} ({most.products_found} found)",
            f"{cheapest.scraper_name} (${cheapest.cost_per_product():.5f})" if cheapest else "[dim]-[/dim]",
        )

    _console.print()
    _console.print(source_table)

    # ── Overall success rates per scraper ──
    by_scraper: dict[str, list[ScraperMetrics]] = defaultdict(list)
    for m in metrics:
        by_scraper[m.scraper_name].append(m)

    rate_table = Table(
        title="Overall Success Rate per Scraper",
        box=box.MARKDOWN,
        header_style="bold magenta",
    )
    rate_table.add_column("Scraper")
    rate_table.add_column("Runs",            justify="right")
    rate_table.add_column("Successes",       justify="right")
    rate_table.add_column("Success rate",    justify="right")
    rate_table.add_column("Avg products",    justify="right")
    rate_table.add_column("Avg time (s)",    justify="right")
    rate_table.add_column("Total cost",      justify="right")

    for scraper_name, rows in sorted(by_scraper.items()):
        successes = [r for r in rows if r.success]
        n = len(rows)
        s = len(successes)
        rate = f"{s}/{n} ({100*s//n}%)"
        avg_prod = f"{sum(r.products_found for r in rows)/n:.1f}" if n else "0"
        avg_time = f"{sum(r.time_seconds for r in rows)/n:.1f}" if n else "0"
        total_cost = sum(r.cost_usd for r in rows)

        rate_table.add_row(
            scraper_name,
            str(n),
            str(s),
            f"[green]{rate}[/green]" if s == n else f"[yellow]{rate}[/yellow]",
            avg_prod,
            avg_time,
            _fmt_cost(total_cost),
        )

    _console.print()
    _console.print(rate_table)
    _console.print()


# ── JSON persistence ──────────────────────────────────────────────────────────

def save_json(metrics: list[ScraperMetrics], path: Path | None = None) -> Path:
    """
    Write metrics to benchmark/results/{YYYY-MM-DD}.json.

    Returns the path written.
    """
    if path is None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = _RESULTS_DIR / f"{today}.json"

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_runs": len(metrics),
        "runs": [m.to_dict() for m in metrics],
        "summary": _build_summary_dict(metrics),
    }

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    _console.print(f"[bold]JSON report saved ->[/bold] {path}")
    return path


def _build_summary_dict(metrics: list[ScraperMetrics]) -> dict[str, Any]:
    """Compute aggregate summary dict embedded in the JSON report."""
    by_scraper: dict[str, list[ScraperMetrics]] = defaultdict(list)
    by_source:  dict[str, list[ScraperMetrics]] = defaultdict(list)
    for m in metrics:
        by_scraper[m.scraper_name].append(m)
        by_source[m.source].append(m)

    scraper_stats = {}
    for name, rows in by_scraper.items():
        n = len(rows)
        s = len([r for r in rows if r.success])
        scraper_stats[name] = {
            "runs": n,
            "successes": s,
            "success_rate": round(s / n, 3) if n else 0,
            "avg_products_found": round(sum(r.products_found for r in rows) / n, 1) if n else 0,
            "avg_time_seconds":   round(sum(r.time_seconds   for r in rows) / n, 2) if n else 0,
            "total_cost_usd":     round(sum(r.cost_usd       for r in rows), 6),
        }

    source_stats = {}
    for src, rows in by_source.items():
        successful = [r for r in rows if r.success and r.products_found > 0]
        source_stats[src] = {
            "fastest_scraper": min(rows, key=lambda r: r.time_seconds).scraper_name if rows else None,
            "most_products_scraper": max(rows, key=lambda r: r.products_found).scraper_name if rows else None,
            "max_products_found": max(r.products_found for r in rows) if rows else 0,
        }

    return {
        "by_scraper": scraper_stats,
        "by_source": source_stats,
        "total_cost_usd": round(sum(m.cost_usd for m in metrics), 6),
    }
