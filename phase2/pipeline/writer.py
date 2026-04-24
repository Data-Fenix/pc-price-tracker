"""Write processed product data to output/processed/."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from config import settings

PROCESSED_DIR = settings.OUTPUT_DIR / "processed"


def write_jsonl(records: list[dict[str, Any]], name: str, run_date: date | None = None) -> Path:
    """Write records as JSON Lines to output/processed/{name}/{date}.jsonl."""
    today = run_date or datetime.now(timezone.utc).date()
    out_path = PROCESSED_DIR / name / f"{today.isoformat()}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return out_path


def write_csv(records: list[dict[str, Any]], name: str, run_date: date | None = None) -> Path:
    """Write records as CSV to output/processed/{name}/{date}.csv."""
    import pandas as pd

    today = run_date or datetime.now(timezone.utc).date()
    out_path = PROCESSED_DIR / name / f"{today.isoformat()}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(records).to_csv(out_path, index=False, encoding="utf-8")
    return out_path


def write_json(obj: Any, name: str, run_date: date | None = None) -> Path:
    """Write a single JSON object to output/processed/{name}/{date}.json."""
    today = run_date or datetime.now(timezone.utc).date()
    out_path = PROCESSED_DIR / name / f"{today.isoformat()}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    out_path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    return out_path
