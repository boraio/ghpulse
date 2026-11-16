"""Data-quality gates over silver: each gate returns a list of violations, empty when it passes."""

from __future__ import annotations

import argparse
import json
import sys

import backfill
import duckdb
import fetch
from silver import TABLES


def path_exists(con, path: str) -> bool:
    """True when path exists, local or s3://; lists the parent folder, because glob() on an exact S3 path does not check (2026-11-16)."""
    parent = path.rsplit("/", 1)[0]
    rows = con.execute(f"SELECT file FROM glob('{parent}/*')").fetchall()
    return any(f == path for (f,) in rows)


def load_marker(con, path: str) -> dict:
    """One marker as a dict with every field exactly as stored: dt stays a string and hour an int."""
    # Parse the JSON ourselves: read_json guesses types, turning "2026-09-23" into a DATE and, from the path, hour=00 into '00'.
    return json.loads(con.execute(f"SELECT content FROM read_text('{path}')").fetchone()[0])


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
    where = f"{rec.get('dt')} {rec.get('hour')}"
    out = []
    for t in TABLES:
        path = backfill.bronze_file(f"{silver_root}/{t}", rec["dt"], rec["hour"])
        # A missing file is a finding to report, not an error to raise, so look before reading.
        if not path_exists(con, path):
            out.append(f"{where} {t}: file missing ({path})")
            continue
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
        if n != rec.get(f"{t}_rows"):
            out.append(f"{where} {t}: marker says {rec.get(f'{t}_rows')} rows, file holds {n}")
    return out


def main(argv=None) -> int:
    """Run the silver quality gates over an inclusive range of UTC hours; exit 1 on any violation."""
    p = argparse.ArgumentParser(description=main.__doc__)
    p.add_argument("--start", required=True, help="first hour, e.g. 2026-09-22T00 (UTC)")
    p.add_argument("--end", required=True, help="last hour, inclusive")
    p.add_argument("--silver-root", default="s3://ghpulse/silver")
    p.add_argument("--manifest-root", default="s3://ghpulse/_manifest/silver")
    args = p.parse_args(argv)
    keys = backfill.hour_range(backfill.parse_hour(args.start), backfill.parse_hour(args.end))
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    if args.silver_root.startswith("s3://") or args.manifest_root.startswith("s3://"):
        fetch.s3_config(con)

    def exists(path: str) -> bool:
        return path_exists(con, path)

    violations, built = [], []
    for d, h in keys:
        marker = backfill.marker_path(args.manifest_root, d, h)
        if not exists(marker):
            violations.append(f"{d} {h:02d}: no silver marker")
            continue
        rec = load_marker(con, marker)
        violations += marker_violations(rec)
        if rec.get("status") == backfill.INGESTED:
            violations += storage_violations(con, args.silver_root, rec)
            built.append((d, h))
    for t in TABLES:
        files = [backfill.bronze_file(f"{args.silver_root}/{t}", d, h) for d, h in built]
        # Missing files were already reported by the storage gate; the table gate reads only what exists.
        violations += table_violations(con, t, [f for f in files if exists(f)])
    for v in violations:
        print(v)
    print(f"checked {len(keys)} hours, {len(built)} built: {len(violations)} violations")
    return 1 if violations else 0




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


if __name__ == "__main__":
    sys.exit(main())
