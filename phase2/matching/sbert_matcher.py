"""Semantic product matching using Sentence-BERT (all-MiniLM-L6-v2).

Architecture
------------
* Catalog canonical names are encoded once and cached to
  ``output/catalog/embeddings.npy`` (float32, shape [n_unique, 384]).
* Row order is stored in ``output/catalog/embeddings_index.json`` so the
  cache is self-contained and survives catalog re-imports.
* At match time, the query is PREPROCESSED (spec noise stripped) then
  encoded and compared against the category-filtered catalog slice via a
  dot product (valid because both sides are L2-normalised → dot == cosine).
* The same German/English accessory denylist as rule_based is applied first.

Query preprocessing
-------------------
all-MiniLM-L6-v2 is trained on English sentences.  Long German product
listings contain spec noise (8GB, 120Hz, "13-Zoll", "Generalüberholt",
"qwertz") that shifts the embedding away from the clean English catalog
name.  ``_sbert_preprocess()`` strips these patterns BEFORE encoding,
leaving only the brand/model/variant tokens that are semantically
comparable to catalog canonical names.  Catalog names are encoded as-is
(they are already short clean English strings).

Cache invalidation
------------------
The cache is regenerated if:
  * either file is missing, OR
  * the product_id list in the index file differs from the current catalog.
"""
from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

from config import settings
from phase2.matching.rule_based import _ACCESSORY_RE, _BILDSCHIRM_PART_RE

# ---------------------------------------------------------------------------
# Query preprocessor — strips German spec noise before SBERT encoding
# ---------------------------------------------------------------------------
# Goal: map "MacBook Air 13 (2024) Apple M3 mit 8-Core CPU und 8-Core GPU
#        8GB RAM SSD 256GB QWERTZ Deutsch" → "MacBook Air 13 (2024) Apple M3"
# so that the embedding aligns with the clean catalog name.
#
# Rules:
#   strip  — storage/freq specs (8GB, 120Hz), core counts (8-Core),
#             German size (13-Zoll), German function words (mit/und/für…),
#             condition words (Generalüberholt), layout (QWERTZ), language
#             (Deutsch), generic hardware nouns (RAM/SSD/CPU/GPU),
#             display-resolution codes (FHD/4K/3K/QHD).
#   keep   — brand/model names, chip identifiers (M3/i7/RTX), year/gen.

_SBERT_NOISE_RE = re.compile(
    r"\b\d+\s*(?:gb|tb|mb)\b"                   # storage  : 8GB, 256GB, 1TB
    r"|\b\d+\s*(?:mhz|ghz)\b"                   # CPU freq : 3200MHz
    r"|\b\d+\s*hz\b"                             # display  : 120Hz
    r"|\b\d+-?(?:core|zoll|inch)\b"              # hardware : 8-Core, 13-Zoll
    r"|\b(?:mit|und|f[uü]r|aus|von|auf|des|der|die|das)\b"  # German prepositions/articles
    r"|\b(?:chip|kapazit\w{0,6}|qwertz)\b"       # product-condition noise
    r"|\b(?:deutsch|english|englisch)\b"          # keyboard language
    r"|\bgeneral\w{0,3}berholt\b"                # generalüberholt / generaluberholt
    r"|\b(?:ram|ssd|hdd|nvme|cpu|gpu)\b"         # generic hardware nouns
    r"|\b(?:4k|3k|2k|fhd|qhd|uhd|wqxga|wuxga)\b",  # display-resolution codes
    re.IGNORECASE,
)
_WHITESPACE_RE = re.compile(r"\s{2,}")

# Laptop/phone-only: CPU+GPU spec suffixes that are product identifiers for GPU
# catalog entries but are pure component noise in laptop/phone listings.
_LAPTOP_PHONE_NOISE_RE = re.compile(
    r"\b(19|20)\d{2}\b"                                    # release year: 2024, 2019
    r"|\bintel(?:\s+core)?\b"                               # Intel brand / "Intel Core"
    r"|\bi[3579]-?\d{3,6}[a-z]{0,4}\b"                     # Intel CPU: i7-13700H, i5-1235U
    r"|\bultra\s+\d+\b"                                     # Intel Ultra brand: Ultra 7
    r"|\b(?:nvidia|geforce|radeon)\b"                       # GPU brands (laptop component)
    r"|\b(?:rtx|gtx|rx)\s*\d+(?:\s*(?:ti|super|xt))?\b"    # GPU model: RTX 4060 Ti
    r"|\b(?:amd|ryzen|threadripper)\b"                      # AMD CPU brand
    r"|\bai\s+\d+\b"                                        # Ryzen AI notation: AI 9
    r"|\b(?:hx|hs)\s+\d{2,4}\b"                            # Ryzen suffix: HX 370
    r"|\b\d{3,5}[a-z]{1,3}\b"                              # CPU model codes: 7940HS, 155H
    r"|\bgen\s+\d+\b"                                       # Gen notation: Gen 12
    r"|\b(?:oled|amoled|ips)\b"                             # Display panel types
    r"|\b(?:snapdragon|dimensity|mediatek|exynos|kirin)\b"  # Phone SoC brands
    r"|\b\d+\s*gen\s*\d+\b"                               # Gen pattern: 8 Gen 3
    r"|\b(?:retina|display|liquid)\b",                     # Apple/display marketing words
    re.IGNORECASE,
)

_CATALOG_DIR  = settings.OUTPUT_DIR / "catalog"
_EMB_PATH     = _CATALOG_DIR / "embeddings.npy"
_INDEX_PATH   = _CATALOG_DIR / "embeddings_index.json"
_DEFAULT_MODEL = "all-MiniLM-L6-v2"
DEFAULT_THRESHOLD = 0.75


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class SBERTMatchResult:
    matched: bool
    match_score: float        # cosine similarity  0.0 – 1.0
    canonical_name: Optional[str]
    product_id: Optional[str]
    match_method: str         # "cosine_similarity" | "accessory_filter" | "below_threshold"


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class SBERTMatcher:
    """Match product names against a catalog using SBERT embeddings.

    Parameters
    ----------
    catalog:
        List of product dicts with at minimum ``product_id``,
        ``canonical_name``, ``category``.
    threshold:
        Minimum cosine similarity to accept a match (default 0.75).
    model_name:
        Sentence-Transformers model identifier (default all-MiniLM-L6-v2).
    """

    def __init__(
        self,
        catalog: list[dict],
        threshold: float = DEFAULT_THRESHOLD,
        model_name: str = _DEFAULT_MODEL,
    ) -> None:
        self.threshold  = threshold
        self.model_name = model_name

        # Build a stable, deduplicated product list for the embedding matrix.
        # One row per unique (category, canonical_name), sorted for cache stability.
        self._products: list[dict] = _build_deduped_products(catalog)

        # Load model first — needed for both encoding and query inference.
        t0 = time.perf_counter()
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(model_name)
        print(f"[SBERTMatcher] model loaded in {time.perf_counter()-t0:.2f}s")

        # Load or generate catalog embeddings.
        self._embeddings, self._products = self._load_or_encode()

        # Per-category index: category → list of row indices into _embeddings.
        self._cat_idx: dict[str, list[int]] = defaultdict(list)
        for i, p in enumerate(self._products):
            self._cat_idx[p["category"]].append(i)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match(self, name: str, category: str | None = None) -> SBERTMatchResult:
        """Semantically match *name* against the catalog.

        Parameters
        ----------
        name:     Raw or pre-cleaned product name.
        category: Catalog category to restrict candidates (e.g. ``"laptop"``).
        """
        name_lc = (name or "").lower()

        # Step 1 — accessory filter
        if _ACCESSORY_RE.search(name_lc) or _BILDSCHIRM_PART_RE.search(name_lc):
            return SBERTMatchResult(False, 0.0, None, None, "accessory_filter")

        # Step 2 — strip spec noise then encode query
        query_clean = _sbert_preprocess(name, category) or name
        q_emb: np.ndarray = self._model.encode(
            query_clean, normalize_embeddings=True, show_progress_bar=False
        )

        # Step 3 — restrict to category and compute cosine similarities
        indices = self._cat_idx.get(category, []) if category else list(range(len(self._products)))
        if not indices:
            return SBERTMatchResult(False, 0.0, None, None, "no_candidates")

        sims      = np.dot(self._embeddings[indices], q_emb)  # (n_candidates,)
        best_local = int(np.argmax(sims))
        best_sim   = float(sims[best_local])
        best_prod  = self._products[indices[best_local]]

        if best_sim >= self.threshold:
            return SBERTMatchResult(
                matched=True,
                match_score=best_sim,
                canonical_name=best_prod["canonical_name"],
                product_id=best_prod["product_id"],
                match_method="cosine_similarity",
            )
        return SBERTMatchResult(
            matched=False,
            match_score=best_sim,
            canonical_name=None,
            product_id=None,
            match_method="below_threshold",
        )

    def match_dataframe(
        self,
        df,
        name_col: str = "product_name_clean",
        category_col: str = "category",
    ) -> list[SBERTMatchResult]:
        """Batch-match every row in *df*.

        Batch-encodes all non-accessory queries in a single model call for
        efficiency; returns a list aligned to ``df.index``.
        """
        names      = df[name_col].fillna("").tolist()
        categories = (
            df[category_col].fillna("").tolist()
            if category_col in df.columns
            else [None] * len(df)
        )

        results:    list[Optional[SBERTMatchResult]] = [None] * len(names)
        to_encode_idx:   list[int] = []
        to_encode_names: list[str] = []
        to_encode_cats:  list[str] = []

        for i, (name, cat) in enumerate(zip(names, categories)):
            lc = (name or "").lower()
            if _ACCESSORY_RE.search(lc) or _BILDSCHIRM_PART_RE.search(lc):
                results[i] = SBERTMatchResult(False, 0.0, None, None, "accessory_filter")
            else:
                to_encode_idx.append(i)
                to_encode_names.append(name)
                to_encode_cats.append(cat)

        if to_encode_names:
            preprocessed = [_sbert_preprocess(n, c) or n for n, c in zip(to_encode_names, to_encode_cats)]
            q_embs = self._model.encode(
                preprocessed,
                normalize_embeddings=True,
                batch_size=64,
                show_progress_bar=False,
            )
            for j, (global_i, cat) in enumerate(zip(to_encode_idx, to_encode_cats)):
                q_emb   = q_embs[j]
                indices = self._cat_idx.get(cat, []) if cat else list(range(len(self._products)))
                if not indices:
                    results[global_i] = SBERTMatchResult(False, 0.0, None, None, "no_candidates")
                    continue
                sims       = np.dot(self._embeddings[indices], q_emb)
                best_local = int(np.argmax(sims))
                best_sim   = float(sims[best_local])
                best_prod  = self._products[indices[best_local]]
                if best_sim >= self.threshold:
                    results[global_i] = SBERTMatchResult(
                        True, best_sim,
                        best_prod["canonical_name"], best_prod["product_id"],
                        "cosine_similarity",
                    )
                else:
                    results[global_i] = SBERTMatchResult(
                        False, best_sim, None, None, "below_threshold"
                    )

        return results  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _load_or_encode(self) -> tuple[np.ndarray, list[dict]]:
        if self._cache_valid():
            emb  = np.load(_EMB_PATH)
            meta = json.loads(_INDEX_PATH.read_text(encoding="utf-8"))
            # Reconstruct products list from the cached index order.
            prod_map = {p["product_id"]: p for p in self._products}
            ordered  = [prod_map[m["product_id"]] for m in meta if m["product_id"] in prod_map]
            print(f"[SBERTMatcher] loaded {len(ordered)} embeddings from cache ({_EMB_PATH})")
            return emb, ordered

        print(f"[SBERTMatcher] encoding {len(self._products)} catalog products …")
        t0   = time.perf_counter()
        names = [p["canonical_name"] for p in self._products]
        emb  = self._model.encode(
            names,
            normalize_embeddings=True,
            batch_size=32,
            show_progress_bar=True,
        ).astype(np.float32)
        elapsed = time.perf_counter() - t0

        _CATALOG_DIR.mkdir(parents=True, exist_ok=True)
        np.save(_EMB_PATH, emb)
        meta = [
            {"product_id": p["product_id"], "canonical_name": p["canonical_name"], "category": p["category"]}
            for p in self._products
        ]
        _INDEX_PATH.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[SBERTMatcher] encoded in {elapsed:.2f}s — saved to {_EMB_PATH}")
        return emb, self._products

    def _cache_valid(self) -> bool:
        if not _EMB_PATH.exists() or not _INDEX_PATH.exists():
            return False
        try:
            meta = json.loads(_INDEX_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False
        # Valid when the product_id sequence matches exactly.
        cached_ids  = [m["product_id"] for m in meta]
        current_ids = [p["product_id"] for p in self._products]
        return cached_ids == current_ids


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sbert_preprocess(name: str, category: str | None = None) -> str:
    """Strip spec noise from a product query before SBERT encoding."""
    cleaned = _SBERT_NOISE_RE.sub(" ", name)
    if category in ("laptop", "phone"):
        cleaned = _LAPTOP_PHONE_NOISE_RE.sub(" ", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    # Deduplicate tokens: "mit Apple M3" → "Apple M3" after "mit" strip
    # leaves a second "Apple" when the brand already appeared earlier.
    tokens = cleaned.split()
    seen: set[str] = set()
    deduped: list[str] = []
    for t in tokens:
        if t.lower() not in seen:
            seen.add(t.lower())
            deduped.append(t)
    return " ".join(deduped)


def _build_deduped_products(catalog: list[dict]) -> list[dict]:
    """Return one representative per unique (category, canonical_name), sorted."""
    seen: dict[tuple[str, str], dict] = {}
    for p in catalog:
        key = (p["category"], p["canonical_name"])
        if key not in seen:
            seen[key] = p
    # Stable sort so cache key is deterministic across re-imports.
    return sorted(seen.values(), key=lambda p: (p["category"], p["canonical_name"]))
