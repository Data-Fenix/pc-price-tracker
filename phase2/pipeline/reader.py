"""Read raw product JSON partitions from output/raw/."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Iterator

from config import settings

SOURCES = ("google_shopping", "idealo_de_serpapi", "ebay_de", "amazon_de")
RAW_DIR = settings.OUTPUT_DIR / "raw"


class RawDataReader:
    """Load raw partition files from the output/raw/ tree.

    Parameters
    ----------
    raw_dir:
        Path to the raw data root; defaults to ``output/raw/``.
    """

    def __init__(self, raw_dir: str | Path | None = None) -> None:
        self.raw_dir = Path(raw_dir) if raw_dir else RAW_DIR

    def load_latest(
        self,
        source: str | None = None,
        category: str | None = None,
    ):
        """Return a DataFrame containing the most-recent partition per
        (source, category) pair, optionally filtered by *source* / *category*.

        "Most recent" is determined by the year/month/day path components.
        """
        import pandas as pd

        files = list(self.raw_dir.glob("*/*/*/*/*/products.json"))

        # Pick the latest date file per (source, category).
        best: dict[tuple[str, str], tuple[date, Path]] = {}
        for f in files:
            try:
                rel = f.relative_to(self.raw_dir)
            except ValueError:
                continue
            parts = rel.parts  # source, category, year, month, day, products.json
            if len(parts) != 6:
                continue
            src, cat, year, month, day = parts[:5]
            if source and src != source:
                continue
            if category and cat != category:
                continue
            try:
                run_date = date(int(year), int(month), int(day))
            except ValueError:
                continue
            key = (src, cat)
            if key not in best or run_date > best[key][0]:
                best[key] = (run_date, f)

        all_records: list[dict] = []
        for (src, cat), (run_date, path) in sorted(best.items()):
            try:
                records = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if not isinstance(records, list):
                continue
            for rec in records:
                rec.setdefault("_source", src)
                rec.setdefault("_category", cat)
                rec.setdefault("_run_date", run_date.isoformat())
            all_records.extend(records)

        return pd.DataFrame(all_records) if all_records else pd.DataFrame()

    def load_all(
        self,
        source: str | None = None,
        category: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ):
        """Return a DataFrame with every partition (all dates), optionally filtered."""
        import pandas as pd

        records = list(iter_records(source, category, start_date, end_date))
        return pd.DataFrame(records) if records else pd.DataFrame()


def iter_records(
    source: str | None = None,
    category: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> Iterator[dict]:
    """Yield every product record from matching raw partitions.

    Partition layout: output/raw/{source}/{category}/{year}/{month}/{day}/products.json
    """
    sources = [source] if source else SOURCES
    for src in sources:
        src_dir = RAW_DIR / src
        if not src_dir.exists():
            continue
        for products_file in sorted(src_dir.rglob("products.json")):
            parts = products_file.parts
            try:
                day_idx = len(parts) - 2      # .../{day}/products.json
                run_date = date(
                    int(parts[day_idx - 2]),
                    int(parts[day_idx - 1]),
                    int(parts[day_idx]),
                )
                cat = parts[day_idx - 3]
            except (ValueError, IndexError):
                continue

            if category and cat != category:
                continue
            if start_date and run_date < start_date:
                continue
            if end_date and run_date > end_date:
                continue

            try:
                records = json.loads(products_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue

            if not isinstance(records, list):
                continue

            for rec in records:
                rec.setdefault("_source", src)
                rec.setdefault("_category", cat)
                rec.setdefault("_run_date", run_date.isoformat())
                yield rec


def load_dataframe(
    source: str | None = None,
    category: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """Return all matching records as a pandas DataFrame."""
    import pandas as pd

    records = list(iter_records(source, category, start_date, end_date))
    return pd.DataFrame(records) if records else pd.DataFrame()
