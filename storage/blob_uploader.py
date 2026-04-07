"""
Azure Blob Storage uploader.

Partition scheme
----------------
    raw/{source}/{category}/{year}/{month}/{day}/products.json

Each daily run appends to (or replaces) the blob for that partition.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class BlobUploader:
    """Upload scraped product records to Azure Blob Storage.

    Parameters
    ----------
    container_name:
        Override the container name from settings (useful for testing).
    """

    def __init__(self, container_name: str | None = None) -> None:
        self.container_name = container_name or settings.AZURE_CONTAINER_NAME
        self._client = self._build_client()

    # ── Client construction ────────────────────────────────────────────────────

    def _build_client(self):
        """
        TODO: Instantiate azure.storage.blob.BlobServiceClient.

        Use AZURE_CONNECTION_STRING when available; fall back to
        account name + key credential.
        """
        raise NotImplementedError("BlobUploader._build_client not yet implemented")

    # ── Public API ─────────────────────────────────────────────────────────────

    def upload(
        self,
        records: list[dict[str, Any]],
        *,
        source: str,
        category: str,
        run_date: date | None = None,
    ) -> str:
        """
        Serialise *records* to JSON and upload to the correct blob partition.

        Parameters
        ----------
        records:
            List of ProductRecord dicts to persist.
        source:
            Source key, e.g. ``"amazon_de"``.
        category:
            Category key, e.g. ``"laptops"``.
        run_date:
            Date for the partition path; defaults to today (UTC).

        Returns
        -------
        str
            The full blob path that was written.

        TODO
        ----
        - Build blob path from BLOB_PATH_TEMPLATE.
        - Serialise records with orjson for speed.
        - Call BlobClient.upload_blob(overwrite=True).
        - Log success / failure with logger.
        """
        raise NotImplementedError("BlobUploader.upload not yet implemented")

    def blob_path(self, source: str, category: str, run_date: date) -> str:
        """Return the blob path for the given partition."""
        return settings.BLOB_PATH_TEMPLATE.format(
            source=source,
            category=category,
            year=run_date.strftime("%Y"),
            month=run_date.strftime("%m"),
            day=run_date.strftime("%d"),
        )
