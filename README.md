# PC & Electronics Price Tracker

A daily price scraper for laptops, GPUs, and phones across **Amazon DE**, **eBay DE**, and **Idealo DE**, with output stored in **Azure Blob Storage** using a date-partitioned layout.

## Phase 2 — NLP matching & unified price table

Phase 2 takes the raw scraped records from Phase 1, cleans them, matches each listing against a product catalog using NLP, and writes a unified price table (one record per catalog product, aggregating offers from all sources).

### Matching approaches

| Strategy | Description |
|---|---|
| **rule_based** | Token-overlap ratio between query and catalog canonical name; fast, high-precision, language-agnostic. |
| **fuzzy** | RapidFuzz `token_set_ratio` — handles word-order variation and extra tokens; good for partial matches. |
| **sentence_transformer** | `all-MiniLM-L6-v2` cosine similarity on preprocessed embeddings; best semantic accuracy, especially for noisy German listings. |

### Benchmark results

Test set: 135 positives (45 clean + 45 English-noisy + 45 German-noisy) · 20 accessory negatives.

```
========================================================================
Matcher                 Precision    Recall        F1   FP Rate   ms/rec
========================================================================
rule_based                  0.822     1.000     0.902     0.000      0.0
fuzzy                       0.800     1.000     0.889     0.000      0.1
sentence_transformer        0.837     1.000     0.911     0.000     16.7
========================================================================
```

FP Rate = 0.000 for all matchers — the shared German/English accessory denylist catches all accessory listings before scoring.

### Running the Phase 2 pipeline

```bash
# Full pipeline with SBERT (recommended — best F1)
python run_phase2.py --matcher sbert

# Or with the faster rule-based matcher
python run_phase2.py --matcher rule

# Run only the cleaning step
python run_phase2.py clean
```

Output is written to `output/processed/unified/{category}/{year}/{mm}/{dd}/unified_prices.json`.

### Product catalog

`output/catalog/product_catalog.json` contains 125 synthetic catalog entries (40 laptops, 40 GPUs, 45 phones) hand-crafted to match real scraped product naming patterns from Amazon DE, eBay DE, and Idealo DE. It is committed to the repo as a static asset — no API key required.

---

## Output schema

Each scraped record contains:

| Field | Type | Description |
|---|---|---|
| `product_name` | str | Normalised product title from the listing |
| `price` | float | Numeric price (no currency symbol) |
| `currency` | str | ISO 4217 code, e.g. `EUR` |
| `availability` | str | `in_stock` \| `out_of_stock` \| `unknown` |
| `seller` | str | Merchant / seller name |
| `source` | str | Source key, e.g. `amazon_de` |
| `url` | str | Canonical product URL |
| `scrape_timestamp` | str | ISO-8601 UTC timestamp |
| `category` | str | `laptops` \| `gpus` \| `phones` |

## Blob partition scheme

```
raw/{source}/{category}/{year}/{month}/{day}/products.json
```

Example: `raw/amazon_de/laptops/2026/04/07/products.json`

## Project structure

```
pc-price-tracker/
├── config/
│   ├── settings.py        # Env-var based configuration
│   ├── products.yaml      # Products to track per category
│   └── sources.yaml       # Scraping sources & their settings
├── scrapers/
│   ├── __init__.py
│   └── base_scraper.py    # Abstract BaseScraper
├── storage/
│   ├── __init__.py
│   ├── blob_uploader.py   # Azure Blob Storage backend
│   └── local_storage.py   # Local filesystem backend
├── benchmark/
│   ├── __init__.py
│   ├── runner.py          # Timed benchmark runs
│   ├── metrics.py         # Metric calculations
│   └── report.py          # Console / JSON / Markdown reports
├── utils/
│   ├── __init__.py
│   ├── logger.py          # Structured logging (structlog)
│   └── helpers.py         # Misc helpers (price cleaning, etc.)
├── tests/
│   ├── __init__.py
│   ├── test_scrapers.py
│   └── test_storage.py
├── run_pipeline.py        # CLI entry point
├── requirements.txt
├── .env.example
└── .gitignore
```

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browsers (required for Idealo DE)
playwright install chromium

# 4. Configure environment
cp .env.example .env
# Edit .env with your Azure Storage credentials
```

## Running the pipeline

```bash
# All sources, all categories
python run_pipeline.py

# Specific sources and categories
python run_pipeline.py --sources amazon_de ebay_de --categories laptops gpus

# Dry-run (scrape but skip storage)
python run_pipeline.py --dry-run

# Use local filesystem instead of Azure Blob Storage
python run_pipeline.py --local
```

## Running tests

```bash
pytest tests/ -v
```

## Running benchmarks

```python
from benchmark.runner import BenchmarkRunner
from benchmark.metrics import MetricsCalculator
from benchmark.report import ReportGenerator

results = BenchmarkRunner(sources=["amazon_de"], categories=["laptops"]).run()
summary = MetricsCalculator(results).compute()
ReportGenerator(summary).render_console()
```

## Adding a new scraper

1. Create `scrapers/your_source.py` with a class that extends `BaseScraper`.
2. Implement the `search(product, category) -> list[ProductRecord]` method.
3. Add the source entry to `config/sources.yaml`.
4. Import the class in `scrapers/__init__.py`.

## Adding a new product

Edit `config/products.yaml` and add an entry under the appropriate category:

```yaml
laptops:
  - name: "My New Laptop"
    search_query: "My New Laptop DE"
    brand: BrandName
    model: "Model Name"
```

## Environment variables

| Variable | Description | Default |
|---|---|---|
| `AZURE_STORAGE_CONNECTION_STRING` | Full Azure connection string | — |
| `AZURE_CONTAINER_NAME` | Blob container name | `pc-price-tracker` |
| `REQUEST_TIMEOUT_SECONDS` | HTTP request timeout | `30` |
| `MAX_RETRIES` | Retry attempts on failure | `3` |
| `RETRY_BACKOFF_SECONDS` | Wait between retries | `5` |
| `SCRAPE_DELAY_SECONDS` | Polite delay between requests | `2` |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `LOCAL_OUTPUT_DIR` | Local storage root | `output/` |
