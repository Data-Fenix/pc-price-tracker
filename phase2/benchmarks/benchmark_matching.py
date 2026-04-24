"""Benchmark the three matching strategies on a labelled test set.

Test set
--------
  positives (clean)     — catalog canonical names verbatim (n_per_category × 3 categories)
  positives (noisy_en)  — canonical name + English noise (RAM/SSD suffix)
  positives (noisy_de)  — canonical name + German listing noise + Generalüberholt
  negatives             — 20 hardcoded accessory strings (all caught by the accessory filter)

Metrics
-------
  Precision  — TP / (TP + FP);  FP = accepted accessories + wrong-entry matches
  Recall     — TP / (TP + FN);  FN = positive not matched or matched to wrong catalog entry
  F1         — harmonic mean of precision and recall
  FP Rate    — accessories wrongly matched / total accessories in test set
  ms/rec     — wall-clock milliseconds per query (excludes SBERT model-load time)

Usage
-----
    python -m phase2.benchmarks.benchmark_matching
    python -m phase2.benchmarks.benchmark_matching --n-per-category 20
"""
from __future__ import annotations

import json
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click

from config import settings
from phase2.matching.fuzzy_matcher import FuzzyMatcher
from phase2.matching.rule_based import RuleBasedMatcher
from phase2.matching.sbert_matcher import SBERTMatcher

BENCHMARKS_DIR = settings.OUTPUT_DIR / "benchmarks"
CATALOG_PATH   = settings.OUTPUT_DIR / "catalog/product_catalog.json"

# ---------------------------------------------------------------------------
# Hardcoded accessory negatives — must NEVER match a catalog product.
# Every string here contains at least one term from _ACCESSORY_RE so the
# accessory-filter stage should catch all of them before any scorer runs.
# ---------------------------------------------------------------------------
NEGATIVES: list[tuple[str, str]] = [
    # ── laptop accessories ─────────────────────────────────────────────────
    ("Apple 96W USB-C Netzteil kompatibel mit MacBook Pro 14 Zoll", "laptop"),
    ("Schutzfolie kompatibel mit MacBook Air 13 Retina Display", "laptop"),
    ("Laptoptasche 15.6 Zoll wasserabweisend schwarz mit Trolleyband", "laptop"),
    ("Akku-Pack 65W Li-Ion Ersatz kompatibel mit Lenovo ThinkPad E590", "laptop"),
    ("LCD Touch Screen Display Deckel Bildschirmdeckel 14 Zoll Ersatzteil", "laptop"),
    ("65W Netzteil Ladegerät USB-C kompatibel mit Dell XPS 13 15", "laptop"),
    ("Bildschirmdeckel Gehäusedeckel ASUS VivoBook 15 Ersatzteil", "laptop"),
    ("Li-Pol Akku Versum kompatibel mit Apple MacBook Air 2019 A1932", "laptop"),
    ("Bildschirmschutz Panzerglas Universal 15.6 Zoll Laptop", "laptop"),
    ("Ladekabel USB-C 90W kompatibel mit Lenovo IdeaPad ThinkPad", "laptop"),
    # ── GPU accessories ────────────────────────────────────────────────────
    ("GPU Netzteil Kabel PCIe 8-Pin kompatibel mit NVIDIA RTX 4090", "gpu"),
    ("Ersatzteil Lüfter kompatibel mit NVIDIA GeForce RTX 3080 Founders", "gpu"),
    ("Netzteil 850W 80Plus Gold modular kompatibel mit AMD Radeon RX 7900 XT", "gpu"),
    # ── phone accessories ──────────────────────────────────────────────────
    ("Schutzglas kompatibel mit iPhone 15 Pro Max Displayschutz 9H", "phone"),
    ("Schutzhülle kompatibel mit Samsung Galaxy S24 Ultra Silikon Schwarz", "phone"),
    ("Ladekabel USB-C 2m kompatibel mit Samsung Galaxy schnellladung", "phone"),
    ("Schnellladegerät 45W USB-C Ladegerät kompatibel mit Apple iPhone 15 Pro", "phone"),
    ("Hülle kompatibel mit Google Pixel 8 Pro Stoßfest transparent", "phone"),
    ("Displayfolie Samsung Galaxy S24 FE Anti-Fingerprint schutzfolie Matt", "phone"),
    ("Panzerfolie kompatibel mit Apple iPhone 14 Pro Max 9H Echtglas", "phone"),
]


# ---------------------------------------------------------------------------
# Test-set builder
# ---------------------------------------------------------------------------

def _build_test_set(
    catalog: list[dict],
    n_per_category: int = 15,
    seed: int = 42,
) -> list[tuple[str, str, str | None, str]]:
    """Return list of (query, category, expected_product_id, test_type).

    test_type is one of: "clean" | "noisy_en" | "noisy_de" | "accessory"
    expected_product_id is None for accessory rows.
    """
    rng = random.Random(seed)
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for p in catalog:
        by_cat[p["category"]].append(p)

    cases: list[tuple[str, str, str | None, str]] = []
    for cat, products in sorted(by_cat.items()):
        sample = rng.sample(products, min(n_per_category, len(products)))
        for p in sample:
            pid  = p["product_id"]
            name = p["canonical_name"]
            cases.append((name, cat, pid, "clean"))
            cases.append((f"{name} 16GB RAM 512GB SSD", cat, pid, "noisy_en"))
            cases.append((
                f"2024 {name} mit 16GB RAM 512GB SSD QWERTZ Deutsch Generalüberholt",
                cat, pid, "noisy_de",
            ))

    for query, cat in NEGATIVES:
        cases.append((query, cat, None, "accessory"))

    return cases


# ---------------------------------------------------------------------------
# Per-matcher runner
# ---------------------------------------------------------------------------

def _run_matcher(
    matcher: RuleBasedMatcher | FuzzyMatcher | SBERTMatcher,
    test_cases: list[tuple[str, str, str | None, str]],
) -> dict[str, Any]:
    """Run *matcher* on every test case; return metrics dict."""
    tp = fp = fn = fp_acc = tn_acc = 0
    times: list[float] = []

    for query, cat, expected_id, test_type in test_cases:
        t0 = time.perf_counter()
        res = matcher.match(query, cat)
        times.append(time.perf_counter() - t0)

        if test_type == "accessory":
            if res.matched:
                fp_acc += 1
                fp += 1
            else:
                tn_acc += 1
        else:                               # positive
            if res.matched and res.product_id == expected_id:
                tp += 1
            elif res.matched:
                fp += 1                     # accepted but wrong catalog entry
            else:
                fn += 1

    n_pos = tp + fn
    n_neg = fp_acc + tn_acc

    precision  = tp / (tp + fp) if (tp + fp) else 1.0
    recall     = tp / (tp + fn) if (tp + fn) else 0.0
    f1         = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    fp_rate    = fp_acc / n_neg if n_neg else 0.0
    ms_per_rec = sum(times) / len(times) * 1000 if times else 0.0

    return {
        "tp": tp, "fp": fp, "fn": fn,
        "fp_accessory": fp_acc, "tn_accessory": tn_acc,
        "n_positives": n_pos, "n_negatives": n_neg,
        "precision":  round(precision,  4),
        "recall":     round(recall,     4),
        "f1":         round(f1,         4),
        "fp_rate":    round(fp_rate,    4),
        "ms_per_rec": round(ms_per_rec, 3),
    }


# ---------------------------------------------------------------------------
# Benchmark orchestrator
# ---------------------------------------------------------------------------

def run_benchmark(catalog: list[dict], n_per_category: int = 15) -> dict[str, Any]:
    """Initialise all three matchers and benchmark them; return results dict."""
    click.echo("Building test set …")
    test_cases = _build_test_set(catalog, n_per_category=n_per_category)

    n_pos = sum(1 for *_, t in test_cases if t != "accessory")
    n_neg = sum(1 for *_, t in test_cases if t == "accessory")
    n_clean    = sum(1 for *_, t in test_cases if t == "clean")
    n_noisy_en = sum(1 for *_, t in test_cases if t == "noisy_en")
    n_noisy_de = sum(1 for *_, t in test_cases if t == "noisy_de")
    click.echo(
        f"  {n_pos} positives  "
        f"({n_clean} clean + {n_noisy_en} noisy_en + {n_noisy_de} noisy_de)  ·  "
        f"{n_neg} accessories"
    )

    results: dict[str, Any] = {
        "run_at":         datetime.now(timezone.utc).isoformat(),
        "catalog_size":   len(catalog),
        "n_positives":    n_pos,
        "n_negatives":    n_neg,
        "test_breakdown": {
            "clean": n_clean, "noisy_en": n_noisy_en,
            "noisy_de": n_noisy_de, "accessory": n_neg,
        },
        "matchers": {},
    }

    # ── 1. Rule-based ───────────────────────────────────────────────────────
    click.echo("\n[1/3] rule_based …")
    rb = RuleBasedMatcher(catalog)
    results["matchers"]["rule_based"] = _run_matcher(rb, test_cases)
    m = results["matchers"]["rule_based"]
    click.echo(f"      F1={m['f1']:.3f}  P={m['precision']:.3f}  R={m['recall']:.3f}  "
               f"FP_rate={m['fp_rate']:.3f}  {m['ms_per_rec']:.2f}ms/rec")

    # ── 2. Fuzzy ────────────────────────────────────────────────────────────
    click.echo("[2/3] fuzzy …")
    fz = FuzzyMatcher(catalog)
    results["matchers"]["fuzzy"] = _run_matcher(fz, test_cases)
    m = results["matchers"]["fuzzy"]
    click.echo(f"      F1={m['f1']:.3f}  P={m['precision']:.3f}  R={m['recall']:.3f}  "
               f"FP_rate={m['fp_rate']:.3f}  {m['ms_per_rec']:.2f}ms/rec")

    # ── 3. Sentence-BERT ────────────────────────────────────────────────────
    click.echo("[3/3] sentence_transformer  (loading model …)")
    sb = SBERTMatcher(catalog)
    click.echo("      model ready — running inference …")
    results["matchers"]["sentence_transformer"] = _run_matcher(sb, test_cases)
    m = results["matchers"]["sentence_transformer"]
    click.echo(f"      F1={m['f1']:.3f}  P={m['precision']:.3f}  R={m['recall']:.3f}  "
               f"FP_rate={m['fp_rate']:.3f}  {m['ms_per_rec']:.2f}ms/rec")

    return results


# ---------------------------------------------------------------------------
# Pretty-print table
# ---------------------------------------------------------------------------

def _print_table(results: dict[str, Any]) -> None:
    col_w = 22
    header = (
        f"{'Matcher':<{col_w}}  {'Precision':>9}  {'Recall':>8}  "
        f"{'F1':>8}  {'FP Rate':>8}  {'ms/rec':>7}"
    )
    sep = "=" * len(header)
    click.echo(f"\n{sep}")
    click.echo(header)
    click.echo(sep)
    for name, m in results["matchers"].items():
        click.echo(
            f"{name:<{col_w}}  {m['precision']:>9.3f}  {m['recall']:>8.3f}  "
            f"{m['f1']:>8.3f}  {m['fp_rate']:>8.3f}  {m['ms_per_rec']:>7.1f}"
        )
    click.echo(sep)
    click.echo(
        f"\nTest set: {results['n_positives']} positives  "
        f"({results['test_breakdown']['clean']} clean + "
        f"{results['test_breakdown']['noisy_en']} noisy_en + "
        f"{results['test_breakdown']['noisy_de']} noisy_de)  ·  "
        f"{results['n_negatives']} accessories"
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--n-per-category", default=15, show_default=True,
    help="Number of catalog products sampled per category for positive test cases.",
)
def main(n_per_category: int) -> None:
    """Benchmark rule_based, fuzzy, and sentence_transformer matchers."""
    sys.stdout.reconfigure(encoding="utf-8")

    if not CATALOG_PATH.exists():
        click.echo(f"Catalog not found: {CATALOG_PATH}", err=True)
        raise SystemExit(1)

    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    click.echo(f"Loaded catalog: {len(catalog)} products")

    results = run_benchmark(catalog, n_per_category=n_per_category)

    _print_table(results)

    BENCHMARKS_DIR.mkdir(parents=True, exist_ok=True)
    today    = datetime.now(timezone.utc).date().isoformat()
    out_path = BENCHMARKS_DIR / f"matching_benchmark_{today}.json"
    out_path.write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    click.echo(f"\nResults saved → {out_path}")


if __name__ == "__main__":
    main()
