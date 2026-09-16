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


def fetch_hour(con: duckdb.DuckDBPyConnection, date: str, hour: int) -> int:
    """Download one hour, write it to bronze, return the row count."""
    url = GHARCHIVE.format(date=date, hour=hour)
    target = f"s3://{BUCKET}/bronze/events/dt={date}/hour={hour:02d}/data.parquet"

    print(f"  {url}")
    with tempfile.TemporaryDirectory() as tmp:
        gz = Path(tmp) / "events.json.gz"
        raw = Path(tmp) / "events.json"

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

        # Decompress rather than letting DuckDB read the .gz directly: newer
        # GH Archive files are concatenated gzip members, which some readers
        # truncate at the first member without erroring — silently losing
        # most of the hour.
        with gzip.open(gz, "rb") as fin, open(raw, "wb") as fout:
            while chunk := fin.read(1 << 20):
                fout.write(chunk)

        # union_by_name: event payloads differ by type, and the schema drifts
        # as GitHub adds fields. Without it, a file whose first rows lack a
        # column fails on a later row that has it.
        #
        # Do NOT add maximum_depth here. Limiting it stops actor and repo
        # being parsed as structs too, so actor.login returns a quoted JSON
        # value ('"dave[bot]"') rather than a plain string — and every
        # string comparison against it silently stops matching.
        con.execute(f"""
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
                FROM read_json_auto('{raw}', union_by_name=true, ignore_errors=true)
            ) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE 1)
        """)

        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{target}')"
        ).fetchone()[0]
        print(f"    {rows:,} events  ({size_mb:.1f} MB gz)  -> {target}")
        return rows


def main() -> None:
    if len(sys.argv) < 3:
        sys.exit(__doc__)

    # Normalise the date: GH Archive paths are zero-padded for the month but
    # NOT for the hour (2026-09-01-5.json.gz, not -05). Getting either wrong
    # yields a 404 that looks like missing data.
    try:
        date = datetime.date.fromisoformat(sys.argv[1]).isoformat()
    except ValueError:
        sys.exit(f"bad date {sys.argv[1]!r} — expected YYYY-MM-DD")
    start = int(sys.argv[2])
    end = int(sys.argv[3]) if len(sys.argv) > 3 else start

    con = duckdb.connect()
    s3_config(con)

    print(f"ghpulse: {date} hours {start}..{end} -> s3://{BUCKET}/bronze/")
    total = sum(fetch_hour(con, date, h) for h in range(start, end + 1))
    print(f"\ntotal: {total:,} events")


if __name__ == "__main__":
    main()
