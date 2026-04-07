"""
Unit / integration tests for scraper classes.

Test plan
---------
- test_base_scraper_make_record        – _make_record returns all required fields
- test_base_scraper_blob_path_format   – blob path matches partition scheme
- test_amazon_de_scraper_search        – mock HTTP, assert records returned
- test_ebay_de_scraper_search          – mock HTTP, assert records returned
- test_idealo_de_scraper_search        – mock Playwright, assert records returned
- test_scraper_retry_on_http_error     – 503 triggers retry, succeeds on 2nd attempt
- test_scraper_empty_results           – search with no results returns []

TODO: implement using pytest + responses (for requests mocking).
"""
import pytest


@pytest.mark.skip(reason="not yet implemented")
def test_base_scraper_make_record():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_scraper_retry_on_http_error():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_amazon_de_scraper_search():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_ebay_de_scraper_search():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_idealo_de_scraper_search():
    pass


@pytest.mark.skip(reason="not yet implemented")
def test_scraper_empty_results():
    pass
