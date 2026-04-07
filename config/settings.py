"""
Central configuration loaded from environment variables and .env file.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
OUTPUT_DIR = Path(os.getenv("LOCAL_OUTPUT_DIR", BASE_DIR / "output"))

LOG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Azure Blob Storage ─────────────────────────────────────────────────────────
AZURE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_ACCOUNT_NAME: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
AZURE_ACCOUNT_KEY: str = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
AZURE_CONTAINER_NAME: str = os.getenv("AZURE_CONTAINER_NAME", "pc-price-tracker")

# ── Blob partition template ────────────────────────────────────────────────────
# raw/{source}/{category}/{year}/{month}/{day}/products.json
BLOB_PATH_TEMPLATE = "raw/{source}/{category}/{year}/{month}/{day}/products.json"

# ── Scraper behaviour ──────────────────────────────────────────────────────────
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", 30))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
RETRY_BACKOFF: int = int(os.getenv("RETRY_BACKOFF_SECONDS", 5))
SCRAPE_DELAY: float = float(os.getenv("SCRAPE_DELAY_SECONDS", 2))

HTTP_PROXY: str = os.getenv("HTTP_PROXY", "")
HTTPS_PROXY: str = os.getenv("HTTPS_PROXY", "")

PROXIES: dict = {}
if HTTP_PROXY:
    PROXIES["http"] = HTTP_PROXY
if HTTPS_PROXY:
    PROXIES["https"] = HTTPS_PROXY

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE: str = os.getenv("LOG_FILE", str(LOG_DIR / "pipeline.log"))

# ── Product & source config files ─────────────────────────────────────────────
PRODUCTS_CONFIG: Path = CONFIG_DIR / "products.yaml"
SOURCES_CONFIG: Path = CONFIG_DIR / "sources.yaml"

# ── Output schema fields (ordered) ────────────────────────────────────────────
OUTPUT_FIELDS = [
    "product_name",
    "price",
    "currency",
    "availability",
    "seller",
    "source",
    "url",
    "scrape_timestamp",
    "category",
]
