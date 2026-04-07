"""
Azure Blob Storage uploader.

Partition scheme
----------------
    raw/{source}/{category}/{year}/{month}/{day}/products.json

Each daily scrape run overwrites that partition's blob so the latest data
is always authoritative.  Use ``upload_all`` to backfill an entire local
data tree.

Dry-run mode
------------
    BlobUploader(dry_run=True)

Logs what would be uploaded without touching Azure — safe when credentials
are absent or during CI.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# Lazy import so the package is optional when running without Azure creds
try:
    from azure.core.exceptions import AzureError
    from azure.storage.blob import BlobServiceClient, ContentSettings
    _AZURE_AVAILABLE = True
except ImportError:
    _AZURE_AVAILABLE = False
    AzureError = Exception  # type: ignore[assignment,misc]


class BlobUploader:
    """Upload product records to Azure Blob Storage.

    Parameters
    ----------
    container_name:
        Override the container name from settings (useful for testing).
    dry_run:
        When True, log intended operations without touching Azure.
    """

    def __init__(
        self,
        container_name: str | None = None,
        dry_run: bool = False,
    ) -> None:
        self.container_name = container_name or settings.AZURE_CONTAINER_NAME
        self.dry_run = dry_run
        self._client: Any = None   # BlobServiceClient, built lazily

        if not dry_run:
            if not _AZURE_AVAILABLE:
                logger.warning(
                    "azure_sdk_missing",
                    note="Install azure-storage-blob to enable uploads; falling back to dry_run=True",
                )
                self.dry_run = True
            elif not (settings.AZURE_CONNECTION_STRING or settings.AZURE_ACCOUNT_NAME):
                logger.warning(
                    "azure_creds_missing",
                    note="Set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME in .env; falling back to dry_run=True",
                )
                self.dry_run = True
            else:
                self._client = self._build_client()

    # ── Client construction ────────────────────────────────────────────────────

    def _build_client(self) -> "BlobServiceClient":
        """Instantiate BlobServiceClient from settings."""
        if settings.AZURE_CONNECTION_STRING:
            return BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
        from azure.storage.blob import StorageSharedKeyCredential
        credential = StorageSharedKeyCredential(
            settings.AZURE_ACCOUNT_NAME,
            settings.AZURE_ACCOUNT_KEY,
        )
        account_url = f"https://{settings.AZURE_ACCOUNT_NAME}.blob.core.windows.net"
        return BlobServiceClient(account_url=account_url, credential=credential)

    # ── Blob path helper ───────────────────────────────────────────────────────

    def blob_path(self, source: str, category: str, run_date: date) -> str:
        """Return the blob path for the given partition."""
        return settings.BLOB_PATH_TEMPLATE.format(
            source=source,
            category=category,
            year=run_date.strftime("%Y"),
            month=run_date.strftime("%m"),
            day=run_date.strftime("%d"),
        )

    # ── Upload a single local file ─────────────────────────────────────────────

    def upload(
        self,
        local_path: Path,
        *,
        source: str,
        category: str,
        run_date: date | None = None,
    ) -> str:
        """
        Upload *local_path* to the correct blob partition.

        Parameters
        ----------
        local_path:
            Path to the local products.json file.
        source:
            Source key, e.g. ``"amazon_de"``.
        category:
            Category key, e.g. ``"laptops"``.
        run_date:
            Date for the partition; defaults to today (UTC).

        Returns
        -------
        str
            The blob path written (or that would have been written in dry_run).
        """
        today = run_date or datetime.now(timezone.utc).date()
        blob_name = self.blob_path(source, category, today)

        if self.dry_run:
            logger.info(
                "blob_upload_dry_run",
                local_path=str(local_path),
                blob=blob_name,
                container=self.container_name,
            )
            return blob_name

        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        data = local_path.read_bytes()
        self._upload_with_retry(blob_name, data)

        blob_url = (
            f"https://{settings.AZURE_ACCOUNT_NAME}.blob.core.windows.net"
            f"/{self.container_name}/{blob_name}"
        ) if settings.AZURE_ACCOUNT_NAME else f"container={self.container_name}/{blob_name}"

        logger.info(
            "blob_upload_done",
            blob=blob_name,
            bytes=len(data),
            url=blob_url,
        )
        return blob_name

    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _upload_with_retry(self, blob_name: str, data: bytes) -> None:
        """Inner upload wrapped with tenacity retry (3 attempts, exponential back-off)."""
        blob_client = self._client.get_blob_client(
            container=self.container_name,
            blob=blob_name,
        )
        blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/json"),
        )

    # ── Upload records directly (in-memory convenience) ────────────────────────

    def upload_records(
        self,
        records: list[dict[str, Any]],
        *,
        source: str,
        category: str,
        run_date: date | None = None,
    ) -> str:
        """
        Save *records* locally (via LocalStorage) then upload to blob.

        Returns the blob path written.
        """
        from storage.local_storage import LocalStorage
        today = run_date or datetime.now(timezone.utc).date()
        ls = LocalStorage()
        local_path = ls.save(records, source=source, category=category, run_date=today)
        return self.upload(local_path, source=source, category=category, run_date=today)

    # ── Bulk backfill ──────────────────────────────────────────────────────────

    def upload_all(self, data_dir: Path | None = None) -> list[str]:
        """
        Walk *data_dir* and upload every ``products.json`` found.

        Expects the tree to match:
            data_dir/{source}/{category}/{year}/{month}/{day}/products.json

        Returns a list of blob paths that were processed.
        """
        root = Path(data_dir) if data_dir else settings.OUTPUT_DIR / "raw"
        if not root.exists():
            logger.warning("upload_all_dir_missing", path=str(root))
            return []

        uploaded: list[str] = []
        for json_file in sorted(root.rglob("products.json")):
            try:
                parts = json_file.relative_to(root).parts
                # parts: (source, category, year, month, day, products.json)
                source, category, year, month, day = (
                    parts[0], parts[1], parts[2], parts[3], parts[4]
                )
                run_date = date(int(year), int(month), int(day))
            except (ValueError, IndexError):
                logger.warning("upload_all_bad_path", path=str(json_file))
                continue

            try:
                blob_name = self.upload(
                    json_file,
                    source=source,
                    category=category,
                    run_date=run_date,
                )
                uploaded.append(blob_name)
            except Exception as exc:
                logger.error("upload_all_failed", path=str(json_file), error=str(exc))

        logger.info(
            "upload_all_done",
            files_processed=len(uploaded),
            dry_run=self.dry_run,
        )
        return uploaded
