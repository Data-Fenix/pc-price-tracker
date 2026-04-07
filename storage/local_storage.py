"""
Local filesystem storage — mirrors the Azure blob partition scheme.

Writes to:
    {OUTPUT_DIR}/raw/{source}/{category}/{year}/{month}/{day}/products.json

Useful for local development and as a fallback when Azure is unavailable.
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

        Parameters
        ----------
        records:
            List of ProductRecord dicts.
        source:
            Source key, e.g. ``"amazon_de"``.
        category:
            Category key, e.g. ``"laptops"``.
        run_date:
            Date for the partition; defaults to today (UTC).

        Returns
        -------
        Path
            Absolute path of the file that was written.

        TODO
        ----
        - Build directory path mirroring the blob partition scheme.
        - Create intermediate directories with mkdir(parents=True).
        - Write JSON with json.dumps and utf-8 encoding.
        - Log the output path.
        """
        raise NotImplementedError("LocalStorage.save not yet implemented")

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
