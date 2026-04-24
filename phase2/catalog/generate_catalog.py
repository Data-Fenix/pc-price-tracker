"""Generate a master product catalog from all raw scraped data.

PORTFOLIO ARTIFACT — not executed as part of the live pipeline.

Reads every partition under output/raw/, deduplicates across sources using
the three-stage matching strategy (rule → fuzzy → SBERT), and writes:
  output/catalog/catalog.json   — full records with offer lists
  output/catalog/catalog.csv    — flat summary (one row per unique product)

Usage (manual):
    python -m phase2.catalog.generate_catalog [--threshold 88]
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import click

from config import settings
from phase2.cleaning.cleaner import clean_records
from phase2.matching import fuzzy_matcher, rule_based, sbert_matcher
from phase2.merging.merger import merge_groups
from phase2.pipeline.reader import iter_records

CATALOG_DIR = settings.OUTPUT_DIR / "catalog"


def build_catalog(
    fuzzy_threshold: int = 88,
    sbert_threshold: float = 0.82,
    use_sbert: bool = True,
) -> list[dict]:
    """Load all raw data, run the full matching pipeline, return catalog records."""
    raw = list(iter_records())
    if not raw:
        click.echo("No raw records found under output/raw/")
        return []

    records = clean_records(raw)
    click.echo(f"Cleaned {len(records)} / {len(raw)} records")

    # Stage 1 — rule-based (exact identifiers)
    rule_groups = rule_based.match(records)
    click.echo(f"Rule-based groups: {len(rule_groups)}")

    # Stage 2 — fuzzy title matching on remaining singletons
    fuzzy_groups = fuzzy_matcher.match(records, threshold=fuzzy_threshold)
    click.echo(f"Fuzzy groups: {len(fuzzy_groups)}")

    # Stage 3 — SBERT semantic matching (optional; slow on first run)
    sbert_groups: list[list[int]] = []
    if use_sbert:
        click.echo("Running SBERT matching (downloads model on first run)…")
        sbert_groups = sbert_matcher.match(records, threshold=sbert_threshold)
        click.echo(f"SBERT groups: {len(sbert_groups)}")

    all_groups = rule_groups + fuzzy_groups + sbert_groups
    catalog = merge_groups(records, all_groups, matched_by="catalog_build")
    return catalog


def write_catalog(catalog: list[dict]) -> None:
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)

    json_path = CATALOG_DIR / "catalog.json"
    json_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding="utf-8")
    click.echo(f"Wrote {len(catalog)} products → {json_path}")

    csv_path = CATALOG_DIR / "catalog.csv"
    flat_fields = ["product_name", "currency", "price_min", "price_max", "price_mean", "offer_count", "matched_by"]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=flat_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(catalog)
    click.echo(f"Wrote CSV summary → {csv_path}")


@click.command()
@click.option("--fuzzy-threshold", default=88, show_default=True, help="Fuzzy match score cutoff (0–100)")
@click.option("--sbert-threshold", default=0.82, show_default=True, help="SBERT cosine similarity cutoff")
@click.option("--no-sbert", is_flag=True, default=False, help="Skip SBERT (faster, less accurate)")
def main(fuzzy_threshold: int, sbert_threshold: float, no_sbert: bool) -> None:
    """Build and write the master product catalog."""
    catalog = build_catalog(
        fuzzy_threshold=fuzzy_threshold,
        sbert_threshold=sbert_threshold,
        use_sbert=not no_sbert,
    )
    if catalog:
        write_catalog(catalog)


if __name__ == "__main__":
    main()
