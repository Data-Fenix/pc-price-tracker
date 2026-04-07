"""
Local filesystem storage — mirrors the Azure blob partition scheme.

Writes to:
    {OUTPUT_DIR}/raw/{source}/{category}/{year}/{month}/{day}/products.json

If the file already exists for today, new records are merged with the
existing ones.  Deduplication is done by the ``url`` field; incoming
records overwrite stale ones for the same URL so that price updates are
always reflected.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class LocalStorage:
    """Persist product records to the local filesystem.

    Parameters
    ----------
    base_dir:
        Root output directory; defaults to ``settings.OUTPUT_DIR``.
    """

    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else settings.OUTPUT_DIR

    # ── Public API ─────────────────────────────────────────────────────────────

    def save(
        self,
        records: list[dict[str, Any]],
        *,
        source: str,
        category: str,
        run_date: date | None = None,
    ) -> Path:
        """
        Write *records* as pretty-printed JSON to the local partition path.

        If a file already exists for this (source, category, date) partition,
        the new records are merged into it.  Deduplication is by ``url``; a
        blank/missing URL is treated as unique so those records are always
        appended rather than replaced.

        Returns the absolute path of the file written.
        """
        if not records:
            logger.warning("local_storage_no_records", source=source, category=category)

        today = run_date or datetime.now(timezone.utc).date()
        path = self.output_path(source, category, today)
        path.parent.mkdir(parents=True, exist_ok=True)

        merged = self._merge(path, records)

        path.write_text(
            json.dumps(merged, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(
            "local_storage_saved",
            source=source,
            category=category,
            path=str(path),
            total_records=len(merged),
            new_records=len(records),
        )
        return path

    def output_path(self, source: str, category: str, run_date: date) -> Path:
        """Return the file path for the given partition."""
        return (
            self.base_dir
            / "raw"
            / source
            / category
            / run_date.strftime("%Y")
            / run_date.strftime("%m")
            / run_date.strftime("%d")
            / "products.json"
        )

    # ── Internal ───────────────────────────────────────────────────────────────

    @staticmethod
    def _merge(
        path: Path,
        incoming: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Merge *incoming* records into any existing file at *path*.

        - Index existing records by URL.
        - Overwrite index entries with incoming records that share the same URL
          (price update wins).
        - Records with empty/missing URL are always appended (no dedup key).
        - Return merged list preserving existing order, new records at end.
        """
        existing: list[dict[str, Any]] = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(existing, list):
                    existing = []
            except (json.JSONDecodeError, OSError):
                existing = []

        url_to_idx: dict[str, int] = {}
        for idx, rec in enumerate(existing):
            url = (rec.get("url") or "").strip()
            if url:
                url_to_idx[url] = idx

        for rec in incoming:
            url = (rec.get("url") or "").strip()
            if url and url in url_to_idx:
                existing[url_to_idx[url]] = rec       # overwrite stale price
            elif url:
                url_to_idx[url] = len(existing)
                existing.append(rec)
            else:
                existing.append(rec)                  # no URL — always append

        return existing
