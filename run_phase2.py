"""Phase 2 pipeline entry point.

Usage
-----
    python run_phase2.py --matcher sbert          # full pipeline with SBERT (default)
    python run_phase2.py full --matcher sbert      # same as above
    python run_phase2.py full --matcher rule       # rule-based matcher
    python run_phase2.py full --matcher fuzzy      # fuzzy matcher
    python run_phase2.py clean                     # clean raw data and print summary
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import click
import pandas as pd

from config import settings
from phase2.cleaning.cleaner import Cleaner
from phase2.pipeline.reader import iter_records

CATALOG_PATH  = settings.OUTPUT_DIR / "catalog/product_catalog.json"
PROCESSED_DIR = settings.OUTPUT_DIR / "processed"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_catalog() -> list[dict]:
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"Catalog not found: {CATALOG_PATH}")
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def _init_matcher(name: str, catalog: list[dict], threshold: float | None):
    """Return an initialised matcher instance."""
    if name == "rule":
        from phase2.matching.rule_based import RuleBasedMatcher
        kwargs = {"threshold": threshold} if threshold is not None else {}
        return RuleBasedMatcher(catalog, **kwargs)
    if name == "fuzzy":
        from phase2.matching.fuzzy_matcher import FuzzyMatcher
        # fuzzy threshold is 0–100; caller may pass 0.0–1.0 by mistake
        t = (threshold * 100 if threshold is not None and threshold <= 1.0
             else threshold) if threshold is not None else None
        kwargs = {"threshold": t} if t is not None else {}
        return FuzzyMatcher(catalog, **kwargs)
    # sbert (default)
    from phase2.matching.sbert_matcher import SBERTMatcher
    kwargs = {"threshold": threshold} if threshold is not None else {}
    return SBERTMatcher(catalog, **kwargs)


def _clean_for_json(obj):
    """Recursively replace NaN/Inf → None and numpy/Timestamp types → native Python."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(item) for item in obj]
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if hasattr(obj, "isoformat"):           # datetime / pd.Timestamp
        return obj.isoformat()
    if hasattr(obj, "item"):               # numpy scalar
        val = obj.item()
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    return obj


def _raw_to_cleaned_df(raw: list[dict]) -> pd.DataFrame:
    """Load raw records into a DataFrame and run the Cleaner."""
    df = pd.DataFrame(raw)
    # Ensure 'category' column exists (raw records have it directly as "laptops" etc.)
    if "category" not in df.columns and "_category" in df.columns:
        df["category"] = df["_category"]
    return Cleaner().clean(df)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------

@click.group(invoke_without_command=True)
@click.option(
    "--matcher",
    type=click.Choice(["rule", "fuzzy", "sbert"], case_sensitive=False),
    default=None,
    help="Shortcut: run the full pipeline with this matcher.",
)
@click.pass_context
def cli(ctx: click.Context, matcher: str | None) -> None:
    """PC Price Tracker — Phase 2 pipeline."""
    if ctx.invoked_subcommand is None:
        if matcher:
            ctx.invoke(full, matcher=matcher)
        else:
            click.echo(ctx.get_help())


# ---------------------------------------------------------------------------
# clean subcommand
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--source", default=None, help="Restrict to one raw source folder")
@click.option("--category", default=None, help="Restrict to one category")
def clean(source: str | None, category: str | None) -> None:
    """Clean raw records and print a summary."""
    sys.stdout.reconfigure(encoding="utf-8")
    raw = list(iter_records(source=source, category=category))
    click.echo(f"Loaded {len(raw)} raw records")
    if not raw:
        return
    cleaned = _raw_to_cleaned_df(raw)
    click.echo(f"Cleaned: {len(cleaned)} records kept  "
               f"({len(raw) - len(cleaned)} dropped)")
    click.echo(f"  anomalies flagged : {int(cleaned['is_anomaly'].sum())}")
    click.echo(f"  by category       : {cleaned['category'].value_counts().to_dict()}")


# ---------------------------------------------------------------------------
# full subcommand  (clean → match → merge → write)
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--matcher",
    type=click.Choice(["rule", "fuzzy", "sbert"], case_sensitive=False),
    default="sbert", show_default=True,
    help="Matching strategy to use.",
)
@click.option("--source",    default=None, help="Restrict to one raw source folder")
@click.option("--category",  default=None, help="Restrict to one category")
@click.option("--threshold", default=None, type=float,
              help="Override default match threshold.")
def full(
    matcher: str,
    source: str | None,
    category: str | None,
    threshold: float | None,
) -> None:
    """Run the complete clean → match → merge → write pipeline."""
    sys.stdout.reconfigure(encoding="utf-8")
    click.echo(f"=== Phase 2 pipeline: full run  [matcher={matcher}] ===")

    # ── 1. Load & clean ──────────────────────────────────────────────────────
    click.echo("\n[1/3] Loading and cleaning raw records …")
    raw = list(iter_records(source=source, category=category))
    if not raw:
        click.echo("No raw records found — run scrapers first.")
        return

    cleaned_df = _raw_to_cleaned_df(raw)
    click.echo(
        f"  {len(raw)} raw → {len(cleaned_df)} cleaned  "
        f"({int(cleaned_df['is_anomaly'].sum())} anomalies flagged)"
    )

    # ── 2. Match against catalog ─────────────────────────────────────────────
    click.echo(f"\n[2/3] Loading catalog and running {matcher} matcher …")
    catalog = _load_catalog()
    click.echo(f"  catalog: {len(catalog)} products")

    m = _init_matcher(matcher, catalog, threshold)
    match_results = m.match_dataframe(cleaned_df)

    records = cleaned_df.to_dict(orient="records")
    matched: list[dict] = []
    n_acc = n_below = 0
    for rec, res in zip(records, match_results):
        if res.matched:
            rec["product_id"]     = res.product_id
            rec["canonical_name"] = res.canonical_name
            rec["match_score"]    = float(res.match_score)
            rec["match_method"]   = res.match_method
            matched.append(rec)
        elif res.match_method == "accessory_filter":
            n_acc += 1
        else:
            n_below += 1

    click.echo(
        f"  {len(matched)} matched  ·  {n_acc} accessories filtered  "
        f"·  {n_below} below threshold"
    )
    if not matched:
        click.echo("No records matched — nothing to write.")
        return

    # ── 3. Merge groups ───────────────────────────────────────────────────────
    click.echo("\n[3/3] Merging groups and writing unified output …")
    from phase2.merging.merger import merge_product_group

    by_product: dict[str, list[dict]] = defaultdict(list)
    for rec in matched:
        by_product[rec["product_id"]].append(rec)

    unified = [
        merge_product_group(pid, recs, matcher)
        for pid, recs in sorted(by_product.items())
    ]
    click.echo(f"  {len(unified)} unique catalog products")

    # ── 4. Write partitioned output ───────────────────────────────────────────
    today = datetime.now(timezone.utc).date()
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for u in unified:
        by_cat[u["category"]].append(u)

    total_written = 0
    for cat, products in sorted(by_cat.items()):
        out_path = (
            PROCESSED_DIR / "unified" / cat
            / str(today.year)
            / f"{today.month:02d}"
            / f"{today.day:02d}"
            / "unified_prices.json"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(_clean_for_json(products), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        total_price_obs = sum(len(p["prices"]) for p in products)
        click.echo(
            f"  {cat}: {len(products)} products  "
            f"({total_price_obs} price observations) → {out_path}"
        )
        total_written += len(products)

    click.echo(f"\n=== Done: {total_written} unified products written ===")


if __name__ == "__main__":
    cli()
