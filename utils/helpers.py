"""
Miscellaneous helper utilities for the price-tracker pipeline.

Planned helpers
---------------
- clean_price(raw: str) -> float
    Strip currency symbols, thousands separators, and convert German decimal
    notation (comma) to float.

- normalise_availability(raw: str) -> str
    Map source-specific availability strings to canonical values:
    "in_stock" | "out_of_stock" | "unknown".

- slugify(text: str) -> str
    Convert a product name to a URL/filesystem-safe slug.

- chunk(lst: list, size: int) -> Iterator[list]
    Yield successive fixed-size chunks from a list (useful for batch uploads).
"""
