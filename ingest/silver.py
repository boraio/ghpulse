"""Silver layer: typed, deduplicated, bot-filtered tables built from bronze, one per event type."""

from __future__ import annotations

TABLES = ("pull_requests", "issues", "stars", "reviews")


def silver_sql(table: str) -> str:
    """Return the SELECT that builds one silver table from a relation named `bronze` (stub)."""
    return "SELECT 1 AS id"
