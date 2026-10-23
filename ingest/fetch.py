#!/usr/bin/env python3
"""
fetch.py — one hour of GH Archive into the bronze layer.

Downloads a single hourly file, converts it to Parquet, and writes it to
object storage partitioned by date and hour.

Idempotent: re-running the same hour overwrites that partition and touches
nothing else. Bronze is immutable per-partition, not append-only, which is
what makes reprocessing safe.

Usage:
    s3env && python3 ingest/fetch.py 2026-09-14 12
    s3env && python3 ingest/fetch.py 2026-09-14 0 23     # a whole day
    s3env && python3 ingest/fetch.py --dry-run 2026-09-14 12
"""
import datetime
import gzip
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

GHARCHIVE = "https://data.gharchive.org/{date}-{hour}.json.gz"

# GH Archive rejects urllib's default user-agent ("Python-urllib/3.x") with
# 403 Forbidden, while the same URL returns 200 to curl. The failure looks
# exactly like a missing file, which sends you hunting for the wrong thing.
USER_AGENT = "ghpulse/0.1 (+https://github.com/boraio/ghpulse)"
BUCKET = os.environ.get("GHPULSE_BUCKET", "ghpulse")

# Normalise the date: GH Archive paths are zero-padded for the month but
# NOT for the hour (2026-09-01-5.json.gz, not -05). Getting either wrong
# yields a 404 that looks like missing data.
def gharchive_url(date: str, hour: int) -> str:
    return GHARCHIVE.format(date=date, hour=hour)


def bronze_path(date: str, hour: int) -> str:
    return f"s3://{BUCKET}/bronze/events/dt={date}/hour={hour:02d}/data.parquet"


def s3_config(con: duckdb.DuckDBPyConnection) -> None:
    """Point DuckDB at RadosGW.

    path-style addressing is mandatory: RadosGW does not do virtual-host
    style, and the failure mode is a misleading credentials error rather
    than anything mentioning the URL layout.
    """
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://10.0.20.252:80")
    host = endpoint.split("://", 1)[-1]
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}'")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_use_ssl={'true' if endpoint.startswith('https') else 'false'}")
    for key, setting in (
        ("AWS_ACCESS_KEY_ID", "s3_access_key_id"),
        ("AWS_SECRET_ACCESS_KEY", "s3_secret_access_key"),
    ):
        val = os.environ.get(key)
        if not val:
            sys.exit(f"{key} not set — run `s3env` first")
        con.execute(f"SET {setting}='{val}'")


def transform_sql(src, target, date, hour) -> str:
    # union_by_name: event payloads differ by type, and the schema drifts
    # as GitHub adds fields. Without it, a file whose first rows lack a
    # column fails on a later row that has it.
    #
    # Do NOT add maximum_depth here. Limiting it stops actor and repo
    # being parsed as structs too, so actor.login returns a quoted JSON
    # value ('"dave[bot]"') rather than a plain string — and every
    # string comparison against it silently stops matching.
    return f"""
        COPY (
            SELECT
                id, type, actor, repo,
                -- payload as JSON TEXT, not a struct.
                -- read_json_auto merges every event type's payload into
                -- one giant struct — PushEvent's `commits` alongside
                -- IssuesEvent's `issue` alongside PullRequestEvent's
                -- `pull_request`, ~200 fields, nearly all NULL per row.
                -- Beyond being wasteful, DuckDB crashes with
                -- "INTERNAL Error: Unsupported type for
                -- NumericValueUnionToValue" when querying it.
                -- JSON preserves everything and commits to nothing;
                -- silver extracts typed columns per event type.
                CAST(payload AS JSON) AS payload,
                public,
                CAST(created_at AS TIMESTAMP) AS created_at,
                '{date}' AS dt,
                {hour} AS hour
            FROM read_json_auto('{src}', union_by_name=true, ignore_errors=true)
        ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE 1)
    """

def fetch_hour(con: duckdb.DuckDBPyConnection, date: str, hour: int, dry_run: bool) -> int:
    """Download one hour, write it to bronze, return the row count."""
    url = gharchive_url(date, hour)
    target = bronze_path(date, hour)

    print(f"  {url}")
    if dry_run:
        print(f"    DRY-RUN: Target path would be {target}")
        return 0

    with tempfile.TemporaryDirectory() as tmp:
        gz = Path(tmp) / "events.json.gz"
        plain = Path(tmp) / "events.json"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req) as resp, open(gz, "wb") as out:
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
        except urllib.error.HTTPError as e:
            # GH Archive occasionally has gaps. A missing hour is a fact
            # about the source, not a failure of this pipeline — record it
            # and move on rather than aborting a backfill.
            print(f"    SKIP: {e.code} {e.reason}")
            return 0

        size_mb = gz.stat().st_size / 1024 / 1024
        if size_mb < 0.1:
            print(f"    SKIP: suspiciously small ({size_mb:.2f} MB)")
            return 0

        # Create a plain-text copy of the decompressed file for debugging
        # purposes. This allows us to inspect the raw JSON data using tools
        # like head and jq when DuckDB's type inference fails on a bad hour.
        # Note that DuckDB can read .gz files natively, so this step costs
        # approximately 10x the disk space.
        with gzip.open(gz, "rb") as fin, open(plain, "wb") as fout:
            while chunk := fin.read(1 << 20):
                fout.write(chunk)

        con.execute(transform_sql(plain, target, date, hour))

        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{target}')"
        ).fetchone()[0]
        print(f"    {rows:,} events  ({size_mb:.1f} MB gz)  -> {target}")
        return rows


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Fetch GH Archive data and write to bronze layer.")
    parser.add_argument("--dry-run", action="store_true", help="Print the URL and target path and exit without downloading or writing anything.")
    parser.add_argument("date", help="Date in YYYY-MM-DD format")
    parser.add_argument("start_hour", type=int, help="Start hour (0-23)")
    parser.add_argument("end_hour", type=int, nargs="?", default=None, help="End hour (0-23), defaults to start_hour if not provided")

    args = parser.parse_args()

    # Normalise the date: GH Archive paths are zero-padded for the month but
    # NOT for the hour (2026-09-01-5.json.gz, not -05). Getting either wrong
    # yields a 404 that looks like missing data.
    try:
        date = datetime.date.fromisoformat(args.date).isoformat()
    except ValueError:
        sys.exit(f"bad date {args.date!r} — expected YYYY-MM-DD")
    start = args.start_hour
    end = args.end_hour if args.end_hour is not None else start

    con = duckdb.connect()
    s3_config(con)

    print(f"ghpulse: {date} hours {start}..{end} -> s3://{BUCKET}/bronze/")
    total = sum(fetch_hour(con, date, h, args.dry_run) for h in range(start, end + 1))
    print(f"\ntotal: {total:,} events")


if __name__ == "__main__":
    main()
