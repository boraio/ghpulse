"""Backfill logic for GH Archive hours, keyed by (date_iso, hour) in UTC: planning, error classification, retry timing, line counting and downloading. Downloads take an injectable opener, so tests/test_transform.py stays offline."""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import http.client
import json
import multiprocessing
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zlib
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
import fetch


def parse_hour(s: str) -> dt.datetime:
    """Parse 'YYYY-MM-DDTHH' (e.g. 2026-09-01T05) into a timezone-aware UTC datetime."""
    naive = dt.datetime.strptime(s, '%Y-%m-%dT%H')
    return dt.datetime(naive.year, naive.month, naive.day, naive.hour, tzinfo=dt.UTC)

def hour_range(start: dt.datetime, end: dt.datetime) -> list[tuple[str, int]]:
    """Inclusive list of (date_iso, hour) pairs, one per hour from start to end."""
    if end < start:
        raise ValueError("end is before start")
    hours = []
    current = start
    while current <= end:
        hours.append((current.strftime('%Y-%m-%d'), current.hour))
        current += dt.timedelta(hours=1)
    return hours

INGESTED, ABSENT, FAILED = "ingested", "absent", "failed"
# Outcomes of classify(). A 404 is reported as ABSENT, reusing the marker status.
RETRY, FAIL, ABORT = "retry", "fail", "abort"

def partition_key(path: str) -> tuple[str, int]:
    """From a path like s3://b/bronze/events/dt=2026-09-01/hour=05/data.parquet return ('2026-09-01', 5)."""
    parts = path.split('/')
    date_part = next((part for part in parts if part.startswith('dt=')), None)
    hour_part = next((part for part in parts if part.startswith('hour=')), None)
    if date_part is None or hour_part is None:
        raise ValueError(f"not a bronze partition path: {path!r}")
    date_iso = date_part.split('=')[1]
    hour = int(hour_part.split('=')[1])
    return (date_iso, hour)

def plan(keys, statuses, present, mode) -> list:
    """Return the hours to process, in input order."""
    if mode not in {"resume", "retry-absent", "force"}:
        raise ValueError(f"Unknown mode: {mode}")
        
    if mode == "force":
        return list(keys)
        
    result = []
    for key in keys:
        status = statuses.get(key)
        # Separate branches on purpose: a merged or/and version of this chain
        # once hid an operator-precedence bug, so SIM114 is suppressed here.
        if status is None:  # noqa: SIM114
            result.append(key)
        elif status == FAILED:  # noqa: SIM114
            result.append(key)
        elif status not in {INGESTED, ABSENT}:  # noqa: SIM114
            result.append(key)
        elif status == INGESTED and key not in present:  # noqa: SIM114
            result.append(key)
        elif status == ABSENT and mode == "retry-absent":
            result.append(key)
        
    return result


def count_lines(chunks) -> int:
    """Count lines across an iterable of bytes chunks. A final line without a trailing newline counts."""
    count = 0
    last_byte = b""
    for chunk in chunks:
        count += chunk.count(b"\n")
        if chunk:
            last_byte = chunk[-1:]
    if last_byte and last_byte != b"\n":
        count += 1
    return count


def backoff_s(attempt: int, retry_after: float | None = None, rng=random.random) -> float:
    """Seconds to wait before retry number attempt (0-based): exponential with jitter, never less than retry_after."""
    delay = min(60.0, 2.0 * 2 ** min(attempt, 10)) * (0.5 + 0.5 * rng())
    if retry_after is not None:
        delay = max(delay, float(retry_after))
    return delay


def too_recent(key: tuple[str, int], now: dt.datetime) -> bool:
    """True if this hour is still inside the publish lag and should not be fetched yet."""
    # The real GH Archive publish lag is unmeasured; 3 hours from the start of the hour is a conservative guess.
    start = dt.datetime.strptime(key[0], "%Y-%m-%d").replace(hour=key[1], tzinfo=dt.UTC)
    return start > now - dt.timedelta(hours=3)


class SuspiciousDownload(Exception):
    """A download that finished without an HTTP error but cannot be trusted: a short read or an implausibly small file."""

def download(url: str, dest, user_agent: str, timeout: float = 60.0, min_bytes: int = 100_000, opener=urllib.request.urlopen) -> int:
    """Stream url to dest and return the bytes written; raise SuspiciousDownload on a short read or a file smaller than min_bytes."""
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    n = 0
    with opener(req, timeout=timeout) as resp, open(dest, "wb") as out:
        expected = resp.headers.get("Content-Length")
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
            n += len(chunk)
    if expected is not None and int(expected) != n:
        raise SuspiciousDownload(f"short read: {n} of {expected} bytes from {url}")
    if n < min_bytes:
        raise SuspiciousDownload(f"only {n} bytes from {url}")
    return n

def decompress(gz, dest) -> int:
    """Stream the gzip file gz into the plain file dest and return its line count, counted the same way as count_lines."""
    with gzip.open(gz, "rb") as fin, open(dest, "wb") as fout:
        def chunks():
            while True:
                chunk = fin.read(1 << 20)
                if not chunk:
                    break
                fout.write(chunk)
                yield chunk

        return count_lines(chunks())


def marker_path(root: str, dt_iso: str, hour: int) -> str:
    """Manifest marker location for one hour; the hour is zero-padded like bronze paths."""
    return f"{root}/dt={dt_iso}/hour={hour:02d}/status.json"

def write_marker(con, path: str, record: dict) -> None:
    """Write one marker record as JSON to path (local or s3://) via DuckDB."""
    if not path.startswith("s3://"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(record, f)
        local = f.name
    try:
        con.execute(f"COPY (SELECT * FROM read_json('{local}')) TO '{path}' (FORMAT JSON)")
    finally:
        os.unlink(local)

def read_marker(con, path: str) -> dict | None:
    """Read one marker back as a dict, or None if it is empty."""
    cols = "{'dt': 'VARCHAR', 'hour': 'INTEGER', 'status': 'VARCHAR', 'rows': 'BIGINT', 'source_lines': 'BIGINT', 'attempts': 'INTEGER', 'error': 'VARCHAR', 'finished_at': 'VARCHAR', 'code_version': 'VARCHAR'}"
    cur = con.execute(f"SELECT * FROM read_json('{path}', columns={cols}, hive_partitioning=false)")
    row = cur.fetchone()
    if row is None:
        return None
    else:
        return dict(zip([d[0] for d in cur.description], row))

def read_state(con, root: str) -> dict:
    """Map (dt, hour) to status for every marker under root; empty when there are none."""
    pattern = f"{root}/*/*/status.json"
    if con.execute(f"SELECT 1 FROM glob('{pattern}') LIMIT 1").fetchall() == []:
        return {}
    else:
        cols = "{'dt': 'VARCHAR', 'hour': 'INTEGER', 'status': 'VARCHAR'}"
        rows = con.execute(f"SELECT dt, hour, status FROM read_json('{pattern}', columns={cols}, hive_partitioning=false)").fetchall()
        return {(dt, hour): status for dt, hour, status in rows}


def present_partitions(con, bronze_root: str) -> set:
    """The (dt, hour) keys of every data.parquet under bronze_root; empty when there are none."""
    rows = con.execute(f"SELECT file FROM glob('{bronze_root}/*/*/data.parquet')").fetchall()
    return {partition_key(f) for (f,) in rows}


def bronze_file(root: str, dt_iso: str, hour: int) -> str:
    """Bronze Parquet location for one hour under root; same layout as fetch.bronze_path."""
    return f"{root}/dt={dt_iso}/hour={hour:02d}/data.parquet"

def ingest_one(con, key, *, bronze_root: str, manifest_root: str, user_agent: str, opener=urllib.request.urlopen, sleep=time.sleep, max_attempts: int = 4, min_bytes: int = 100_000, code_version: str = "unknown") -> dict:
    """Download, decompress and transform one hour, then write its marker after the data; retries per classify(). Returns the marker record. Re-raises on ABORT without writing a marker."""
    dt_iso, hour = key
    target = bronze_file(bronze_root, dt_iso, hour)
    record = {
        "dt": dt_iso,
        "hour": hour,
        "status": FAILED,
        "rows": None,
        "source_lines": None,
        "attempts": 0,
        "error": None,
        "finished_at": None,
        "code_version": code_version,
    }
    for attempt in range(1, max_attempts + 1):
        record["attempts"] = attempt
        try:
            with tempfile.TemporaryDirectory() as workdir:
                gz = os.path.join(workdir, "events.json.gz")
                plain = os.path.join(workdir, "events.json")
                download(fetch.gharchive_url(dt_iso, hour), gz, user_agent, min_bytes=min_bytes, opener=opener)
                source_lines = decompress(gz, plain)
                if not target.startswith("s3://"):
                    os.makedirs(os.path.dirname(target), exist_ok=True)
                con.execute(fetch.transform_sql(plain, target, dt_iso, hour))
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
            record.update(status=INGESTED, rows=rows, source_lines=source_lines, error=None)
            break
        except Exception as exc:
            outcome = classify(exc)
            record["error"] = repr(exc)[:500]
            if outcome == ABORT:
                raise
            if outcome == ABSENT:
                record["status"] = ABSENT
                break
            if outcome == RETRY and attempt < max_attempts:
                value = getattr(exc, "headers", None) and exc.headers.get("Retry-After")
                retry_after = float(value) if value and value.isdigit() else None
                sleep(backoff_s(attempt - 1, retry_after))
                continue
            else:
                record["status"] = FAILED
                break
    record["finished_at"] = dt.datetime.now(dt.UTC).isoformat()
    write_marker(con, marker_path(manifest_root, dt_iso, hour), record)
    return record


def mode_from_flags(force: bool, retry_absent: bool) -> str:
    """The plan() mode selected by the CLI flags; --force wins over --retry-absent."""
    return "force" if force else ("retry-absent" if retry_absent else "resume")

def status_counts(keys, statuses, present) -> dict:
    """Counts per marker status over keys, plus unattempted (no marker) and drift (ingested marker but no data)."""
    counts = {"ingested": 0, "absent": 0, "failed": 0, "unattempted": 0, "drift": 0}
    for key in keys:
        status = statuses.get(key)
        if status is None:
            counts["unattempted"] += 1
        else:
            counts[status] = counts.get(status, 0) + 1
            if status == INGESTED and key not in present:
                counts["drift"] += 1
    return counts

def exit_code(records) -> int:
    """1 if any hour failed, else 0."""
    return 1 if any(r.get("status") == FAILED for r in records) else 0


def code_version() -> str:
    """Short git hash of the checkout, or 'unknown'."""
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=True, cwd=os.path.dirname(os.path.abspath(__file__)))
        return out.stdout.strip() or "unknown"
    except (OSError, subprocess.CalledProcessError):
        return "unknown"

def _worker(job) -> dict:
    """Process-pool entry point: one DuckDB connection per worker, then ingest_one(). Always returns a dict, never raises."""
    key, bronze_root, manifest_root, threads, version = job
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    if bronze_root.startswith("s3://") or manifest_root.startswith("s3://"):
        fetch.s3_config(con)
    try:
        return ingest_one(con, key, bronze_root=bronze_root, manifest_root=manifest_root, user_agent=fetch.USER_AGENT, code_version=version)
    except Exception as exc:
        return {"dt": key[0], "hour": key[1], "status": FAILED, "rows": None, "attempts": None, "error": repr(exc)[:500], "abort": classify(exc) == ABORT}

def run_pool(todo, bronze_root: str, manifest_root: str, workers: int) -> int:
    """Ingest the planned hours with a spawn-based process pool; returns the exit code (2 on abort)."""
    if not todo:
        return 0
    threads = max(1, (os.cpu_count() or 1) // workers)
    version = code_version()
    records = []
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        futures = [pool.submit(_worker, (key, bronze_root, manifest_root, threads, version)) for key in todo]
        for fut in as_completed(futures):
            rec = fut.result()
            records.append(rec)
            print(f"  {rec['dt']} {rec['hour']:02d} {rec['status']} rows={rec.get('rows')} attempts={rec.get('attempts')}")
            if rec.get("abort"):
                print(f"aborting: {rec['error']}", file=sys.stderr)
                pool.shutdown(cancel_futures=True)
                return 2
    absent = sum(1 for r in records if r["status"] == ABSENT)
    if absent > 3:
        print(f"warning: {absent} hours absent upstream", file=sys.stderr)
    print(f"done: {len(records)} hours, {absent} absent, {sum(1 for r in records if r['status'] == FAILED)} failed")
    return exit_code(records)

import argparse
import datetime as dt
import os
import sys

def main(argv=None) -> int:
    """CLI entry point; see --help."""
    parser = argparse.ArgumentParser(description="Resumable GH Archive backfill into bronze, with a manifest per hour.")
    parser.add_argument("--start", required=True, help="first hour, YYYY-MM-DDTHH, UTC")
    parser.add_argument("--end", required=True, help="last hour, inclusive")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retry-absent", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--bronze-root", default=f"s3://{fetch.BUCKET}/bronze/events")
    parser.add_argument("--manifest-root", default=f"s3://{fetch.BUCKET}/_manifest/events")
    args = parser.parse_args(argv)

    if args.force and not args.yes:
        sys.exit("--force overwrites existing partitions; add --yes to confirm")

    keys = hour_range(parse_hour(args.start), parse_hour(args.end))
    now = dt.datetime.now(dt.UTC)
    recent = [k for k in keys if too_recent(k, now)]
    keys = [k for k in keys if not too_recent(k, now)]

    if recent:
        print(f"skipping {len(recent)} hours inside the publish lag", file=sys.stderr)

    con = duckdb.connect()
    if args.bronze_root.startswith("s3://") or args.manifest_root.startswith("s3://"):
        fetch.s3_config(con)

    statuses = read_state(con, args.manifest_root)
    present = present_partitions(con, args.bronze_root)

    if args.status:
        for name, n in status_counts(keys, statuses, present).items():
            print(f"{name:12} {n}")
        return 0

    todo = plan(keys, statuses, present, mode_from_flags(args.force, args.retry_absent))
    print(f"{len(todo)} of {len(keys)} hours to process")

    if args.dry_run:
        for k in todo:
            print(f"  {k[0]} {k[1]:02d}")
        return 0

    return run_pool(todo, args.bronze_root, args.manifest_root, args.workers)


def classify(exc: Exception) -> str:
    """Decide what to do about an error from downloading or transforming one hour."""
    if isinstance(exc, urllib.error.HTTPError):
        code = exc.code
        if code == 404:
            return ABSENT
        elif code in (401, 403):
            return ABORT
        elif code == 429 or code >= 500:
            return RETRY
        else:
            return FAIL
    elif isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException, EOFError, gzip.BadGzipFile, zlib.error, SuspiciousDownload)):
        return RETRY
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
