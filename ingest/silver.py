"""Silver layer: typed, deduplicated, bot-filtered tables built from bronze, one per event type."""

from __future__ import annotations

import datetime as dt
import os

import backfill

TABLES = ("pull_requests", "issues", "stars", "reviews")

_COMMON = "id, created_at, actor.id AS actor_id, actor.login AS actor_login, repo.id AS repo_id, repo.name AS repo_name, dt, hour"

# Payload fields per table. GitHub's slimmed PR payloads (2025-10-07) carry no author, merged flag or size.
_SPEC = {
    "pull_requests": ("PullRequestEvent", ("json_extract_string(payload, '$.action') AS action, "
                      "CAST(json_extract_string(payload, '$.pull_request.id') AS BIGINT) AS pr_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.number') AS BIGINT) AS pr_number, "
                      "CAST(json_extract_string(payload, '$.pull_request.head.repo.id') AS BIGINT) AS head_repo_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.base.repo.id') AS BIGINT) AS base_repo_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.head.repo.id') AS BIGINT) "
                      "<> CAST(json_extract_string(payload, '$.pull_request.base.repo.id') AS BIGINT) AS from_fork")),
    "issues": ("IssuesEvent", ("json_extract_string(payload, '$.action') AS action, "
               "CAST(json_extract_string(payload, '$.issue.id') AS BIGINT) AS issue_id, "
               "CAST(json_extract_string(payload, '$.issue.number') AS BIGINT) AS issue_number, "
               "json_extract_string(payload, '$.issue.user.login') AS issue_author, "
               "json_extract_string(payload, '$.issue.title') AS title")),
    "stars": ("WatchEvent", ""),
    "reviews": ("PullRequestReviewEvent", ("json_extract_string(payload, '$.action') AS action, "
                "CAST(json_extract_string(payload, '$.pull_request.number') AS BIGINT) AS pr_number, "
                "CAST(json_extract_string(payload, '$.review.id') AS BIGINT) AS review_id, "
                "json_extract_string(payload, '$.review.state') AS state")),
}

# Stability rule from milestone_01_measurements.md: median non-push events per hour in Sep 22-28.
REF_NONPUSH = 16893


def is_stable(n: int, push: int) -> bool:
    """True when an hour looks like normal capture: pushes present, push share 0.50-0.95 and non-push events within 3x of REF_NONPUSH, bounds inclusive."""
    if n <= 0 or push < 1:
        return False
    nonpush = n - push
    return 0.50 <= push / n <= 0.95 and REF_NONPUSH / 3 <= nonpush <= REF_NONPUSH * 3


def silver_sql(table: str) -> str:
    """Return the SELECT that builds one silver table from a relation named `bronze`."""
    if table not in _SPEC:
        raise ValueError(f"unknown silver table: {table!r}")
    event_type, extra = _SPEC[table]
    cols = _COMMON + (", " + extra if extra else "")
    # Dedup on id within one bronze hour: every measured duplicate was within a single hour.
    return (
        f"SELECT {cols} FROM bronze "
        f"WHERE type = '{event_type}' AND actor.login NOT LIKE '%[bot]' "
        "QUALIFY row_number() OVER (PARTITION BY id ORDER BY created_at) = 1"
    )



def silver_one(con, key, *, bronze_root: str, silver_root: str, manifest_root: str, code_version: str = "unknown") -> dict:
    """Build the four silver tables for one bronze hour, then write its marker; returns the marker record."""
    dt_iso, hour = key
    src = backfill.bronze_file(bronze_root, dt_iso, hour)
    con.execute(f"CREATE OR REPLACE TEMP TABLE bronze AS SELECT * FROM read_parquet('{src}', hive_partitioning=false)")
    n, push = con.execute("SELECT count(*), count(*) FILTER (WHERE type = 'PushEvent') FROM bronze").fetchone()
    rec = {"dt": dt_iso, "hour": hour, "status": backfill.INGESTED, "bronze_rows": n, "push": push, "stable": is_stable(n, push)}
    for table in TABLES:
        counts = (
            "SELECT count(*), count(*) FILTER (WHERE actor.login LIKE '%[bot]'), "
            "count(*) FILTER (WHERE actor.login NOT LIKE '%[bot]') "
            "- count(DISTINCT id) FILTER (WHERE actor.login NOT LIKE '%[bot]') "
            f"FROM bronze WHERE type = '{_SPEC[table][0]}'"
        )
        rows_in, bots, dups = con.execute(counts).fetchone()
        out = backfill.bronze_file(f"{silver_root}/{table}", dt_iso, hour)
        if not out.startswith("s3://"):
            os.makedirs(os.path.dirname(out), exist_ok=True)
        con.execute(f"COPY ({silver_sql(table)}) TO '{out}' (FORMAT PARQUET)")
        rows_out = con.execute(f"SELECT count(*) FROM ({silver_sql(table)})").fetchone()[0]
        rec.update({f"{table}_in": rows_in, f"{table}_bots": bots, f"{table}_dups": dups, f"{table}_rows": rows_out})
    rec["finished_at"] = dt.datetime.now(dt.UTC).isoformat(timespec="seconds")
    rec["code_version"] = code_version
    # The marker goes last, so a crash mid-hour leaves data without a marker, which a rerun redoes.
    backfill.write_marker(con, backfill.marker_path(manifest_root, dt_iso, hour), rec)
    return rec
