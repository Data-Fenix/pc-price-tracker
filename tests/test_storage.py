"""
Unit tests for storage backends.

Test plan
---------
- test_local_storage_output_path       – path mirrors blob partition scheme
- test_local_storage_save              – file written, valid JSON, correct fields
- test_local_storage_creates_dirs      – intermediate dirs are created automatically
- test_blob_uploader_blob_path         – blob path matches BLOB_PATH_TEMPLATE
- test_blob_uploader_upload            – mock BlobClient, assert upload_blob called
- test_blob_uploader_fallback_to_local – if Azure unavailable, local fallback used

TODO: implement using pytest + pytest-mock (for Azure SDK mocking).
"""
import pytest
from pathlib import Path
from datetime import date


@pytest.mark.skip(reason="not yet implemented")
def test_local_storage_output_path():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_local_storage_save():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_local_storage_creates_dirs():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_blob_uploader_blob_path():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_blob_uploader_upload():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_blob_uploader_fallback_to_local():
    pass
