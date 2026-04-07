"""
Scrapers package.

Available scraper classes are imported here so that the pipeline can resolve
them by name from sources.yaml without importing each module individually.
"""

from scrapers.base_scraper import BaseScraper  # noqa: F401
