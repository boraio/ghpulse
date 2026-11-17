"""Gold layer: interaction events, the leak-free training aggregate, test targets and repo funnel facts, built from silver."""

from __future__ import annotations


def events_sql() -> str:
    """SELECT of qualifying interaction events over relations pull_requests, issues, stars, reviews and markers (dt, hour, stable)."""
    return "SELECT 1 AS x"  # stub: wrong on purpose, so every check fails before the implementation
