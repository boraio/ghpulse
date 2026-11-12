"""Data-quality gates over silver: each gate returns a list of violations, empty when it passes."""

from __future__ import annotations

import backfill
from silver import TABLES


def marker_violations(rec: dict) -> list[str]:
    """Violations in one silver marker: a status other than ingested, missing per-table counts, or in != bots + dups + rows."""
    where = f"{rec.get('dt')} {rec.get('hour')}"
    if rec.get("status") != backfill.INGESTED:
        return [f"{where}: status is {rec.get('status')!r}, not {backfill.INGESTED!r}"]
    out = []
    for t in TABLES:
        counts = [rec.get(f"{t}_{k}") for k in ("in", "bots", "dups", "rows")]
        if None in counts:
            out.append(f"{where} {t}: per-table counts missing")
        elif counts[0] - counts[1] - counts[2] != counts[3]:
            out.append(f"{where} {t}: in {counts[0]} - bots {counts[1]} - dups {counts[2]} != rows {counts[3]}")
    return out
