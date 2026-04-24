"""Clean and normalise raw product records in six ordered steps.

Step 1 — Category normalisation  : plural → singular ("laptops" → "laptop")
Step 2 — Name normalisation      : strip HTML/emoji, unicode-normalise, lowercase
Step 3 — Price standardisation   : parse German locale strings, coerce to float
Step 4 — Availability mapping    : free-text → in_stock / out_of_stock / unknown
Step 5 — Anomaly flagging        : price > 3× or < 1/3 of category median
Step 6 — Deduplication           : drop (name_clean, source, price_clean) dupes,
                                   keeping the most-recent scrape_timestamp
"""
from __future__ import annotations

import re
import unicodedata

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATEGORY_NORMALIZE: dict[str, str] = {
    "laptops": "laptop",
    "gpus": "gpu",
    "phones": "phone",
    "laptop": "laptop",
    "gpu": "gpu",
    "phone": "phone",
}

# Availability strings that map to each canonical value.
_INSTOCK_PATTERNS = re.compile(
    r"\bin[_\s]?stock\b|verfügbar|lieferbar|available|lagernd", re.I
)
_OUTSTOCK_PATTERNS = re.compile(
    r"\bout[_\s]?of[_\s]?stock\b|nicht verfügbar|ausverkauft|unavailable", re.I
)

# Regex to strip anything that isn't a letter, digit, or useful punctuation.
# Keeps: Unicode letters/digits (\w), whitespace, hyphen, dot, comma, slash,
# parentheses, double-quote, apostrophe.
_KEEP_CHARS_RE = re.compile(r"[^\w\s\-.,\"/\'()]", flags=re.UNICODE)
_WHITESPACE_RE = re.compile(r"\s+")

# German / English price string  e.g. "1.299,99 €"  or  "1,299.99"
_PRICE_STRING_RE = re.compile(r"[\d.,]+")

# Anomaly thresholds
ANOMALY_HIGH_FACTOR = 3.0
ANOMALY_LOW_FACTOR  = 1 / 3


class Cleaner:
    """Apply the six-step cleaning pipeline to a raw products DataFrame."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a cleaned copy of *df* with new normalised columns added."""
        df = df.copy()

        df = self._normalize_category(df)    # step 1
        df = self._normalize_names(df)       # step 2
        df = self._standardize_prices(df)    # step 3
        df = self._standardize_availability(df)  # step 4
        df = self._flag_anomalies(df)        # step 5
        df = self._deduplicate(df)           # step 6

        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Step 1 — Category normalisation
    # ------------------------------------------------------------------

    def _normalize_category(self, df: pd.DataFrame) -> pd.DataFrame:
        df["category"] = (
            df["category"]
            .fillna("")
            .str.lower()
            .map(lambda x: CATEGORY_NORMALIZE.get(x, x))
        )
        return df

    # ------------------------------------------------------------------
    # Step 2 — Name normalisation
    # ------------------------------------------------------------------

    def _normalize_names(self, df: pd.DataFrame) -> pd.DataFrame:
        df["product_name_clean"] = df["product_name"].apply(_clean_name)
        # Blank-out truly empty names so downstream callers can use .isna()
        df.loc[df["product_name_clean"] == "", "product_name_clean"] = pd.NA
        return df

    # ------------------------------------------------------------------
    # Step 3 — Price standardisation
    # ------------------------------------------------------------------

    def _standardize_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        df["price_clean"] = df["price"].apply(_parse_price)
        return df

    # ------------------------------------------------------------------
    # Step 4 — Availability standardisation
    # ------------------------------------------------------------------

    def _standardize_availability(self, df: pd.DataFrame) -> pd.DataFrame:
        df["availability_clean"] = df["availability"].apply(_map_availability)
        return df

    # ------------------------------------------------------------------
    # Step 5 — Anomaly flagging
    # ------------------------------------------------------------------

    def _flag_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        df["is_anomaly"] = False

        valid = df["price_clean"].notna() & df["category"].notna()
        if not valid.any():
            return df

        # Compute per-category median on non-anomalous, positive prices.
        for cat, group in df[valid].groupby("category"):
            median = group["price_clean"].median()
            if median <= 0:
                continue
            high = df.index[
                valid
                & (df["category"] == cat)
                & (df["price_clean"] > ANOMALY_HIGH_FACTOR * median)
            ]
            low = df.index[
                valid
                & (df["category"] == cat)
                & (df["price_clean"] < ANOMALY_LOW_FACTOR * median)
            ]
            df.loc[high, "is_anomaly"] = True
            df.loc[low,  "is_anomaly"] = True

        return df

    # ------------------------------------------------------------------
    # Step 6 — Deduplication
    # ------------------------------------------------------------------

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Sort so the most recent scrape is first.
        df = df.sort_values("scrape_timestamp", ascending=False, na_position="last")
        dedup_keys = ["product_name_clean", "source", "price_clean"]
        # Only dedup rows where all three keys are non-null.
        has_keys = df[dedup_keys].notna().all(axis=1)
        deduped = df[has_keys].drop_duplicates(subset=dedup_keys, keep="first")
        remainder = df[~has_keys]
        return pd.concat([deduped, remainder], ignore_index=True)


# ---------------------------------------------------------------------------
# Module-level convenience wrapper
# ---------------------------------------------------------------------------

def clean_records(records: list[dict]) -> list[dict]:
    """Clean a list of raw record dicts and return the cleaned list."""
    import pandas as pd
    if not records:
        return []
    df = pd.DataFrame(records)
    return Cleaner().clean(df).to_dict(orient="records")


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------

def _clean_name(raw: object) -> str:
    if not isinstance(raw, str) or not raw.strip():
        return ""
    name = raw
    # Strip HTML tags
    name = re.sub(r"<[^>]+>", " ", name)
    # Unicode normalise (also expands ligatures, non-breaking spaces, etc.)
    name = unicodedata.normalize("NFKC", name)
    # Normalise Unicode hyphens (‐ ‑ ‒ – — ―) → standard hyphen-minus
    name = re.sub(r"[‐-―−]", "-", name)
    # Drop emoji and other non-useful symbols; keep word chars + punctuation
    name = _KEEP_CHARS_RE.sub(" ", name)
    # Lowercase
    name = name.lower()
    # Collapse whitespace
    name = _WHITESPACE_RE.sub(" ", name).strip()
    return name


def _parse_price(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if float(value) >= 0 else None
    text = str(value).strip()
    m = _PRICE_STRING_RE.search(text.replace("\xa0", ""))
    if not m:
        return None
    candidate = m.group()
    # German decimal comma: "1.299,99" → 1299.99
    if re.search(r",\d{2}$", candidate):
        candidate = candidate.replace(".", "").replace(",", ".")
    else:
        candidate = candidate.replace(",", "")
    try:
        result = float(candidate)
        return result if result >= 0 else None
    except ValueError:
        return None


def _map_availability(value: object) -> str:
    text = str(value) if value is not None else ""
    if _INSTOCK_PATTERNS.search(text):
        return "in_stock"
    if _OUTSTOCK_PATTERNS.search(text):
        return "out_of_stock"
    # Already-canonical values pass through
    if text.lower() in ("in_stock", "out_of_stock", "unknown"):
        return text.lower()
    return "unknown"
