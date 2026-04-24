"""Rule-based product matcher — catalog lookup via token overlap + accessory filter.

Strategy
--------
1. Reject accessories immediately via a German/English keyword denylist.
2. Tokenise the query name (alphanumeric tokens only, lowercased).
3. For each catalog product in the same category, compute:
       score = |query_tokens ∩ catalog_tokens| / |catalog_tokens|
4. Return the best-scoring product if score ≥ THRESHOLD, else no-match.

This matcher is intentionally high-precision / lower-recall.  Fuzzy and SBERT
matchers recover the residual long-tail.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class MatchResult:
    matched: bool
    match_score: float
    canonical_name: Optional[str]
    product_id: Optional[str]
    match_method: str   # "token_overlap" | "accessory_filter" | "below_threshold"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

THRESHOLD = 0.65

# German + English accessory signal words.  A query containing any of these
# is treated as an accessory / spare part and immediately rejected.
_ACCESSORY_RE = re.compile(
    r"\b("
    r"netzteil|ladegerät|ladekabel|ladebuchse|ladeadapter"
    r"|akku(?:versum)?|batterie|akku\s*pack"
    r"|kompatibel\s+mit"
    r"|schutzfolie|schutzglas|displayfolie|panzerfolie"
    r"|schutzhülle|tragetasche|laptoptasche|laptop\s*tasche"
    r"|displaydeckel|gehäusedeckel|gehäuse"
    r"|ersatzteil|ersatz\s*teil"
    r"|li[-\s]?pol|li[-\s]?ion"
    r"|bildschirmschutz"
    r"|hülle\b|tasche\b"
    r"|(\d+\s*w|\d+\s*watt)\s+netzteil"    # "65W Netzteil"
    r"|lcd\s+(touch\s+)?screen\s+display"   # "lcd touch screen display[deckel]"
    r")\b",
    re.IGNORECASE,
)

# Standalone "bildschirm" only qualifies as accessory when paired with a
# display-component word (cover, replacement, LCD…).
_BILDSCHIRM_PART_RE = re.compile(
    r"\bildschirm\b.{0,60}\b(deckel|displaydeckel|lcd|oled|panel|modul|cover)\b",
    re.IGNORECASE | re.DOTALL,
)

# Tokeniser: keep ASCII letters and digits only.
# This intentionally discards German noise tokens (Zoll, Zubehör, Generalüberholt…)
# while retaining model identifiers (m3, x1, g14, 9530 …).
_TOKEN_RE = re.compile(r"[a-z0-9]+")


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class RuleBasedMatcher:
    """Match product names against a pre-loaded catalog.

    Parameters
    ----------
    catalog:
        List of product dicts, each with at minimum ``product_id``,
        ``canonical_name``, ``category``.
    threshold:
        Minimum token-overlap ratio to accept a match (default 0.65).
    """

    def __init__(self, catalog: list[dict], threshold: float = THRESHOLD) -> None:
        self.threshold = threshold
        # Index by category for fast filtering
        self._by_category: dict[str, list[dict]] = defaultdict(list)
        for p in catalog:
            self._by_category[p["category"]].append(p)
        # Pre-compute frozen token sets for every catalog entry
        self._sig: dict[str, frozenset[str]] = {
            p["product_id"]: _signature(p) for p in catalog
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match(self, name: str, category: str | None = None) -> MatchResult:
        """Match *name* against the catalog.

        Parameters
        ----------
        name:
            Product name (raw or pre-cleaned; matched case-insensitively).
        category:
            If supplied, restrict candidates to this catalog category
            (e.g. ``"laptop"``).
        """
        name_lower = name.lower() if name else ""

        # --- Step 1: accessory filter ----------------------------------
        if _ACCESSORY_RE.search(name_lower) or _BILDSCHIRM_PART_RE.search(name_lower):
            return MatchResult(False, 0.0, None, None, "accessory_filter")

        # --- Step 2: token-overlap scoring ----------------------------
        query_tokens = _tokenize(name_lower)
        candidates = self._by_category.get(category, []) if category else [
            p for prods in self._by_category.values() for p in prods
        ]

        best_score = 0.0
        best_product: dict | None = None

        for product in candidates:
            sig = self._sig[product["product_id"]]
            if not sig:
                continue
            overlap = len(sig & query_tokens)
            score = overlap / len(sig)
            if score > best_score:
                best_score = score
                best_product = product

        # --- Step 3: threshold gate -----------------------------------
        if best_score >= self.threshold and best_product is not None:
            return MatchResult(
                matched=True,
                match_score=best_score,
                canonical_name=best_product["canonical_name"],
                product_id=best_product["product_id"],
                match_method="token_overlap",
            )

        return MatchResult(
            matched=False,
            match_score=best_score,
            canonical_name=None,
            product_id=None,
            match_method="below_threshold",
        )

    def match_dataframe(self, df, name_col: str = "product_name_clean",
                        category_col: str = "category") -> list[MatchResult]:
        """Batch-match every row in *df*; returns a list aligned to df.index."""
        return [
            self.match(row[name_col], category=row.get(category_col))
            for _, row in df.iterrows()
        ]


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> set[str]:
    """Return lowercase ASCII alphanumeric tokens."""
    return set(_TOKEN_RE.findall(text.lower()))


def _signature(product: dict) -> frozenset[str]:
    """Tokens of canonical_name used as the catalog signature to match against."""
    return frozenset(_TOKEN_RE.findall(product.get("canonical_name", "").lower()))
