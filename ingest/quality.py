"""Data-quality gates over silver: each gate returns a list of violations, empty when it passes."""

from __future__ import annotations

import backfill
from silver import TABLES


def marker_violations(rec: dict) -> list[str]:
    """Violations in one silver marker: a status other than ingested, missing per-table counts, or in != bots + dups + rows."""
    return ["stub"] * 3  # stub: wrong on purpose, so every check fails before the implementation
