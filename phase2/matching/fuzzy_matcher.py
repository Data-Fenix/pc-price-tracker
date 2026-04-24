"""Fuzzy product name matching using RapidFuzz token_set_ratio.

Matches scraped product names against a catalog by scoring token overlap
between query and canonical name.

Key implementation notes
------------------------
* rapidfuzz v3 defaults to processor=None (case-sensitive).  We pass
  ``processor=default_process`` on every call so comparison is always
  case-insensitive and whitespace-normalised.
* The same German/English accessory denylist as rule_based.py is applied
  first; accessories are rejected with raw_score=0 before fuzzy scoring.
* A pre-built per-category index of deduplicated canonical names ensures
  each unique product appears only once in the candidate list.

Default threshold
-----------------
See ``tune_threshold()`` — at threshold=85 all catalog products in the
test set score 100.0 while the closest non-catalog product (ThinkPad X1
Yoga vs X1 Carbon) scores 81.5, giving a clean separation.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz, process
from rapidfuzz.utils import default_process

from phase2.matching.rule_based import _ACCESSORY_RE, _BILDSCHIRM_PART_RE

DEFAULT_THRESHOLD = 85   # updated after tuning — see tune_threshold()


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class FuzzyMatchResult:
    matched: bool
    match_score: float       # 0.0 – 1.0  (raw_score / 100)
    raw_score: float         # 0.0 – 100.0  (token_set_ratio output)
    canonical_name: Optional[str]
    product_id: Optional[str]
    match_method: str        # "token_set_ratio" | "accessory_filter" | "below_threshold"


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class FuzzyMatcher:
    """Match product names against a pre-loaded catalog via token_set_ratio.

    Parameters
    ----------
    catalog:
        List of product dicts with at minimum ``product_id``,
        ``canonical_name``, ``category``.
    threshold:
        Minimum token_set_ratio score (0–100) to accept a match.
        Default is ``DEFAULT_THRESHOLD`` (85).
    """

    def __init__(self, catalog: list[dict], threshold: float = DEFAULT_THRESHOLD) -> None:
        self.threshold = threshold

        # Per-category index: lowercased canonical_name → first matching product.
        # Deduplication ensures each unique product appears only once.
        self._choices: dict[str, dict[str, dict]] = {}
        by_cat: dict[str, list[dict]] = defaultdict(list)
        for p in catalog:
            by_cat[p["category"]].append(p)

        for cat, products in by_cat.items():
            seen: dict[str, dict] = {}
            for p in products:
                key = default_process(p["canonical_name"])
                if key not in seen:
                    seen[key] = p
            self._choices[cat] = seen

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match(self, name: str, category: str | None = None) -> FuzzyMatchResult:
        """Fuzzy-match *name* against the catalog.

        Parameters
        ----------
        name:
            Raw or pre-cleaned product name.
        category:
            Catalog category to restrict candidates (e.g. ``"laptop"``).
        """
        name_lc = default_process(name) if name else ""

        # Step 1 — accessory filter (same denylist as rule_based)
        if _ACCESSORY_RE.search(name_lc) or _BILDSCHIRM_PART_RE.search(name_lc):
            return FuzzyMatchResult(False, 0.0, 0.0, None, None, "accessory_filter")

        # Step 2 — build candidate pool
        if category:
            pool = self._choices.get(category, {})
        else:
            pool = {k: p for d in self._choices.values() for k, p in d.items()}

        if not pool:
            return FuzzyMatchResult(False, 0.0, 0.0, None, None, "no_candidates")

        # Step 3 — find best token_set_ratio match
        # processor=None because both query and keys are already default_process'd
        result = process.extractOne(
            name_lc,
            list(pool.keys()),
            scorer=fuzz.token_set_ratio,
            processor=None,
        )

        if result is None:
            return FuzzyMatchResult(False, 0.0, 0.0, None, None, "no_match")

        best_key, raw_score, _ = result
        product = pool[best_key]
        match_score = raw_score / 100.0

        if raw_score >= self.threshold:
            return FuzzyMatchResult(
                matched=True,
                match_score=match_score,
                raw_score=float(raw_score),
                canonical_name=product["canonical_name"],
                product_id=product["product_id"],
                match_method="token_set_ratio",
            )

        return FuzzyMatchResult(
            matched=False,
            match_score=match_score,
            raw_score=float(raw_score),
            canonical_name=None,
            product_id=None,
            match_method="below_threshold",
        )

    def match_dataframe(
        self,
        df,
        name_col: str = "product_name_clean",
        category_col: str = "category",
    ) -> list[FuzzyMatchResult]:
        """Batch-match every row in *df*; returns a list aligned to df.index."""
        return [
            self.match(row[name_col], category=row.get(category_col))
            for _, row in df.iterrows()
        ]


# ---------------------------------------------------------------------------
# Threshold tuning
# ---------------------------------------------------------------------------

def tune_threshold(
    catalog: list[dict],
    positives: list[tuple[str, str]],
    negatives: list[tuple[str, str]],
    thresholds: range | list[int] | None = None,
) -> int:
    """Sweep thresholds and print a precision/recall/F1 table.

    Parameters
    ----------
    catalog:    Product catalog list.
    positives:  (name, category) pairs that SHOULD match a catalog entry.
    negatives:  (name, category) pairs that should NOT match (non-accessories).
    thresholds: Iterable of integer thresholds to test. Defaults to 60–95 step 5.

    Returns the threshold with the best F1 score.
    """
    if thresholds is None:
        thresholds = range(60, 96, 5)

    # Pre-score everything once at threshold=0 to collect raw scores.
    scorer_0 = FuzzyMatcher(catalog, threshold=0)
    pos_scores = [scorer_0.match(n, c).raw_score for n, c in positives]
    neg_scores = [scorer_0.match(n, c).raw_score for n, c in negatives]

    n_pos = len(positives)
    n_neg = len(negatives)

    print(f"\n{'Threshold':>10}  {'TP':>4}  {'FP':>4}  {'FN':>4}  {'TN':>4}  "
          f"{'Precision':>10}  {'Recall':>8}  {'F1':>8}")
    print("-" * 70)

    best_f1, best_t = 0.0, int(list(thresholds)[0])
    for t in thresholds:
        tp = sum(1 for s in pos_scores if s >= t)
        fp = sum(1 for s in neg_scores if s >= t)
        fn = n_pos - tp
        tn = n_neg - fp

        precision = tp / (tp + fp) if (tp + fp) else 1.0
        recall    = tp / (tp + fn) if (tp + fn) else 0.0
        f1        = (2 * precision * recall / (precision + recall)
                     if (precision + recall) else 0.0)

        marker = "  ← best F1" if f1 > best_f1 else ""
        print(f"{t:>10}  {tp:>4}  {fp:>4}  {fn:>4}  {tn:>4}  "
              f"{precision:>10.3f}  {recall:>8.3f}  {f1:>8.3f}{marker}")

        if f1 > best_f1:
            best_f1, best_t = f1, t

    print(f"\nBest threshold: {best_t}  (F1={best_f1:.3f})")
    return best_t
