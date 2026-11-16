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


def storage_violations(con, silver_root: str, rec: dict) -> list[str]:
    """Violations between one silver marker and its files: a table file missing, or holding a different row count."""
    return ["stub"] * 3  # stub: wrong on purpose, so every check fails before the implementation


COMMON_REQUIRED = ("id", "created_at", "actor_id", "actor_login", "repo_id", "repo_name", "dt", "hour")
# Columns that must never be null, per table; optional ones (from_fork, head_repo_id, issue_author, title) are not gated.
REQUIRED = {
    "pull_requests": ("action", "pr_id", "pr_number", "base_repo_id"),
    "issues": ("action", "issue_id", "issue_number"),
    "stars": (),
    "reviews": ("action", "pr_number", "review_id", "state"),
}


def table_violations(con, table: str, files: list[str]) -> list[str]:
    """Violations across one table's silver files: null required columns, ids repeated anywhere, [bot] actors, rows outside their partition."""
    if not files:
        return []
    # Load locally first: grouped count(DISTINCT) over remote Parquet has crashed DuckDB before (PROJECT.md).
    con.execute(f"CREATE OR REPLACE TEMP TABLE q AS SELECT * FROM read_parquet({list(files)}, filename=true, hive_partitioning=false)")
    out = []
    for col in COMMON_REQUIRED + REQUIRED[table]:
        n = con.execute(f"SELECT count(*) FROM q WHERE {col} IS NULL").fetchone()[0]
        if n:
            out.append(f"{table}: {n} rows with null {col}")
    dup = con.execute("SELECT count(id) - count(DISTINCT id) FROM q").fetchone()[0]
    if dup:
        out.append(f"{table}: {dup} duplicate ids")
    bots = con.execute("SELECT count(*) FROM q WHERE actor_login LIKE '%[bot]'").fetchone()[0]
    if bots:
        out.append(f"{table}: {bots} rows from [bot] actors")
    misplaced = con.execute(
        "SELECT count(*) FROM q WHERE filename NOT LIKE "
        "('%/dt=' || dt || '/hour=' || lpad(CAST(hour AS VARCHAR), 2, '0') || '/%')"
    ).fetchone()[0]
    if misplaced:
        out.append(f"{table}: {misplaced} rows whose dt/hour do not match their partition path")
    
    return out
