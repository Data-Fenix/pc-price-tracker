"""Merge matched product groups into unified price records.

Each group (list of record indices) produces one merged record with:
  - canonical product_name (longest title in the group)
  - price statistics (min, max, mean) across all offers
  - per-source offer list
  - merge metadata (strategy, group size, matched_by)
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean
from typing import Any


def merge_product_group(
    product_id: str,
    records: list[dict[str, Any]],
    matched_by: str = "unknown",
) -> dict[str, Any]:
    """Build one unified price record from all records matched to *product_id*.

    Parameters
    ----------
    product_id:  Catalog product_id shared by every record in this group.
    records:     Cleaned records with catalog match fields already attached
                 (``product_id``, ``canonical_name``, ``match_score``, …).
    matched_by:  Name of the matcher that produced this group.
    """
    canonical_name = records[0].get("canonical_name", "")
    category       = records[0].get("category", "")

    prices: list[dict[str, Any]] = []
    good_values: list[float] = []

    for r in records:
        price_clean = r.get("price_clean")
        is_anomaly  = bool(r.get("is_anomaly", False))
        if price_clean is not None and not is_anomaly:
            try:
                good_values.append(float(price_clean))
            except (TypeError, ValueError):
                pass

        prices.append({
            "source":           r.get("source") or r.get("_source"),
            "price":            price_clean,
            "currency":         r.get("currency") or "EUR",
            "availability":     r.get("availability_clean") or "unknown",
            "seller":           r.get("seller"),
            "url":              r.get("url"),
            "scrape_timestamp": r.get("scrape_timestamp"),
            "is_anomaly":       is_anomaly,
        })

    return {
        "product_id":     product_id,
        "canonical_name": canonical_name,
        "category":       category,
        "prices":         prices,
        "price_min":      min(good_values) if good_values else None,
        "price_max":      max(good_values) if good_values else None,
        "price_mean":     round(mean(good_values), 2) if good_values else None,
        "offer_count":    len(prices),
        "matched_by":     matched_by,
        "merged_at":      datetime.now(timezone.utc).isoformat(),
    }


def merge_groups(
    records: list[dict[str, Any]],
    groups: list[list[int]],
    matched_by: str = "unknown",
) -> list[dict[str, Any]]:
    """Return one merged record per group.

    Records that appear in no group are returned as-is (singleton merged records).
    """
    merged: list[dict[str, Any]] = []
    grouped_indices: set[int] = {i for g in groups for i in g}

    for group in groups:
        merged.append(_merge_group([records[i] for i in group], matched_by))

    for i, rec in enumerate(records):
        if i not in grouped_indices:
            merged.append(_merge_group([rec], matched_by="none"))

    return merged


def _merge_group(group: list[dict[str, Any]], matched_by: str) -> dict[str, Any]:
    prices = [r["price"] for r in group if isinstance(r.get("price"), (int, float))]
    name = max((r.get("product_name") or "" for r in group), key=len)
    currency = next((r.get("currency") for r in group if r.get("currency")), "EUR")

    offers = [
        {
            "seller": r.get("seller"),
            "source": r.get("source"),
            "price": r.get("price"),
            "currency": r.get("currency"),
            "url": r.get("url"),
            "availability": r.get("availability"),
            "scrape_timestamp": r.get("scrape_timestamp"),
        }
        for r in group
    ]

    return {
        "product_name": name,
        "currency": currency,
        "price_min": min(prices) if prices else None,
        "price_max": max(prices) if prices else None,
        "price_mean": round(mean(prices), 2) if prices else None,
        "offer_count": len(group),
        "offers": offers,
        "matched_by": matched_by,
        "merged_at": datetime.now(timezone.utc).isoformat(),
    }
