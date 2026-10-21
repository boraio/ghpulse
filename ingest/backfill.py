"""Backfill logic for GH Archive hours, keyed by (date_iso, hour) in UTC: planning, error classification, retry timing, line counting and downloading. Downloads take an injectable opener, so tests/test_transform.py stays offline."""
from __future__ import annotations

import datetime as dt
import gzip
import http.client
import json
import os
import random
import tempfile
import urllib.error
import urllib.request
import zlib


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
    return None


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
