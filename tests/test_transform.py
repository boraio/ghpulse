#!/usr/bin/env python3
"""
test_transform.py — prove the JSON→Parquet transform without touching the
network or object storage.

This is the check that matters in CI: GH Archive's payload schema differs
per event type and drifts as GitHub adds fields, so the transform has to
survive rows that do not share a shape. Synthetic events are enough to
catch a regression in that handling, and they run in under a second.

    python tests/test_transform.py
"""
import json
import sys
import tempfile
from pathlib import Path

import duckdb

# One row per event type we care about, deliberately ragged: PushEvent has
# no `action`, WatchEvent has no `pull_request`, and the last row carries a
# field none of the others do. Without union_by_name this fails.
EVENTS = [
    {
        "id": "1", "type": "PushEvent",
        "actor": {"login": "alice"}, "repo": {"name": "a/b"},
        "payload": {"size": 3},
        "public": True, "created_at": "2026-09-14T12:00:01Z",
    },
    {
        "id": "2", "type": "PullRequestEvent",
        "actor": {"login": "bob"}, "repo": {"name": "a/b"},
        "payload": {"action": "opened",
                    "pull_request": {"additions": 10, "deletions": 2}},
        "public": True, "created_at": "2026-09-14T12:00:02Z",
    },
    {
        "id": "3", "type": "WatchEvent",
        "actor": {"login": "carol"}, "repo": {"name": "c/d"},
        "payload": {"action": "started"},
        "public": True, "created_at": "2026-09-14T12:00:03Z",
    },
    {
        "id": "4", "type": "IssuesEvent",
        "actor": {"login": "dave[bot]"}, "repo": {"name": "c/d"},
        "payload": {"action": "opened",
                    "issue": {"number": 7, "title": "a new field appeared"}},
        "public": True, "created_at": "2026-09-14T12:00:04Z",
    },
]

TRANSFORM = """
    COPY (
        SELECT
            id, type, actor, repo,
            CAST(payload AS JSON) AS payload,
            public,
            CAST(created_at AS TIMESTAMP) AS created_at,
            '{date}' AS dt,
            {hour} AS hour
        FROM read_json_auto('{src}', union_by_name=true, ignore_errors=true)
    ) TO '{dst}' (FORMAT PARQUET, COMPRESSION ZSTD)
"""


def check(label: str, got, want) -> bool:
    ok = got == want
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: {got!r}" +
          ("" if ok else f" (expected {want!r})"))
    return ok


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    src, dst = tmp / "events.json", tmp / "out.parquet"
    src.write_text("\n".join(json.dumps(e) for e in EVENTS))

    con = duckdb.connect()
    con.execute(TRANSFORM.format(date="2026-09-14", hour=12, src=src, dst=dst))

    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    src_pq = f"read_parquet('{dst}')"

    results = [
        check("all rows survive ragged schemas",
              q(f"SELECT count(*) FROM {src_pq}"), 4),
        check("partition columns present",
              q(f"SELECT dt FROM {src_pq} LIMIT 1"), "2026-09-14"),
        check("timestamps parse",
              str(q(f"SELECT min(created_at) FROM {src_pq}")),
              "2026-09-14 12:00:01"),
        check("nested struct access (actor.login)",
              q(f"SELECT count(DISTINCT actor.login) FROM {src_pq}"), 4),
        check("payload is JSON text, not a merged struct",
              q(f"SELECT typeof(payload) FROM {src_pq} LIMIT 1"), "JSON"),
        check("JSON path access (payload -> action)",
              q(f"""SELECT payload->>'$.action' FROM {src_pq}
                     WHERE type = 'PullRequestEvent'"""), "opened"),
        check("deep JSON path (pull_request.additions)",
              q(f"""SELECT CAST(payload->>'$.pull_request.additions' AS BIGINT)
                     FROM {src_pq} WHERE type = 'PullRequestEvent'"""), 10),
        check("absent path yields NULL, not an error",
              q(f"""SELECT count(*) FROM {src_pq}
                     WHERE payload->>'$.pull_request.additions' IS NULL"""), 3),
        check("bot filter matches the convention used in queries",
              q(f"SELECT count(*) FROM {src_pq} "
                "WHERE actor.login LIKE '%[bot]'"), 1),
    ]

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
    import backfill

    results += [
        check("hour_range from parse_hour('2026-09-01T23') to parse_hour('2026-09-02T01') equals [('2026-09-01', 23), ('2026-09-02', 0), ('2026-09-02', 1)]",
              backfill.hour_range(backfill.parse_hour('2026-09-01T23'), backfill.parse_hour('2026-09-02T01')),
              [('2026-09-01', 23), ('2026-09-02', 0), ('2026-09-02', 1)]),
        check("partition_key('s3://b/bronze/events/dt=2026-09-01/hour=05/data.parquet') equals ('2026-09-01', 5)",
              backfill.partition_key('s3://b/bronze/events/dt=2026-09-01/hour=05/data.parquet'),
              ('2026-09-01', 5)),
        check("hour_range with end before start raises ValueError",
              True,
              True),
    ]

    try:
        backfill.hour_range(backfill.parse_hour('2026-09-02T01'), backfill.parse_hour('2026-09-01T23'))
        results[-1] = False
    except ValueError:
        results[-1] = True

    K = [("2026-09-01", h) for h in range(5)]
    marked = {K[0]: backfill.INGESTED, K[1]: backfill.ABSENT, K[2]: backfill.FAILED, K[3]: backfill.INGESTED}
    present = {K[0]}

    results += [
        check("backfill.plan(K, marked, present, 'resume') equals [K[2], K[3], K[4]]",
              backfill.plan(K, marked, present, "resume"),
              [K[2], K[3], K[4]]),
        check("backfill.plan(K, marked, present, 'retry-absent') equals [K[1], K[2], K[3], K[4]]",
              backfill.plan(K, marked, present, "retry-absent"),
              [K[1], K[2], K[3], K[4]]),
        check("backfill.plan(K, marked, present, 'force') equals K",
              backfill.plan(K, marked, present, "force"),
              K),
        check("backfill.parse_hour('2026-09-01T05') equals backfill.dt.datetime(2026, 9, 1, 5, tzinfo=backfill.dt.timezone.utc)",
              backfill.parse_hour('2026-09-01T05'),
              backfill.dt.datetime(2026, 9, 1, 5, tzinfo=backfill.dt.timezone.utc)),
    ]

    results += [
        check("resume redoes a FAILED hour even if its data exists",
              backfill.plan([K[2]], {K[2]: backfill.FAILED}, {K[2]}, "resume"),
              [K[2]]),
        check("retry-absent redoes a FAILED hour even if its data exists",
              backfill.plan([K[2]], {K[2]: backfill.FAILED}, {K[2]}, "retry-absent"),
              [K[2]]),

        check("force returns a copy, not the caller's list", backfill.plan(K, {}, set(), "force") is not K, True),
        check("unknown status is redone", backfill.plan([K[0]], {K[0]: "mystery"}, {K[0]}, "resume"), [K[0]]),
        check("count_lines: no trailing newline", backfill.count_lines([b"a\nb"]), 2),
        check("count_lines: across chunks", backfill.count_lines([b"a\n", b"b\n"]), 2),
        check("count_lines: line split across chunks", backfill.count_lines([b"a", b"b\n"]), 1),
        check("count_lines: empty input", backfill.count_lines([]), 0),
        check("backoff doubles per attempt", [backfill.backoff_s(0, rng=lambda: 1.0), backfill.backoff_s(2, rng=lambda: 1.0)], [2.0, 8.0]),
        check("backoff jitter floor is half", backfill.backoff_s(0, rng=lambda: 0.0), 1.0),
        check("backoff is capped at 60 seconds", backfill.backoff_s(10, rng=lambda: 1.0), 60.0),
        check("backoff honours a longer Retry-After", backfill.backoff_s(0, retry_after=30, rng=lambda: 1.0), 30.0),
        check("Retry-After never shortens the wait", backfill.backoff_s(5, retry_after=1, rng=lambda: 1.0), 60.0),
        check("hours inside the publish lag are refused", [backfill.too_recent(("2026-09-01", 10), backfill.parse_hour("2026-09-01T12")), backfill.too_recent(("2026-09-01", 9), backfill.parse_hour("2026-09-01T12")), backfill.too_recent(("2026-09-01", 5), backfill.parse_hour("2026-09-01T12"))], [True, False, False]),
    ]

    import gzip
    import urllib.error
    import http.client
    import zlib

    def http_error(code):
        return urllib.error.HTTPError("https://example.invalid", code, "x", {}, None)

    C = backfill.classify

    import io

    class FakeResponse:
        def __init__(self, body, length):
            self._body = io.BytesIO(body)
            self.headers = {"Content-Length": str(length)}

        def read(self, n=-1):
            return self._body.read(n)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    seen = {}

    def fake_opener(body, length):
        def opener(req, timeout):
            seen["ua"] = req.get_header("User-agent")
            return FakeResponse(body, length)
        return opener

    def make_gz(path, data, cut=False):
        with gzip.open(path, "wb") as f:
            f.write(data)
        if cut:
            blob = path.read_bytes()
            path.write_bytes(blob[: len(blob) // 2])
        return path

    class FakeCon:
        def __init__(self):
            self.sql = []

        def execute(self, sql):
            self.sql.append(sql)
            return self

    def raises(exc_type, fn):
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    results += [
        check("404 is recorded absent", C(http_error(404)), backfill.ABSENT),
        check("401 and 403 abort the run", [C(http_error(401)), C(http_error(403))], [backfill.ABORT, backfill.ABORT]),
        check("429 and 503 retry", [C(http_error(429)), C(http_error(503))], [backfill.RETRY, backfill.RETRY]),
        check("other 4xx fails without retry", C(http_error(400)), backfill.FAIL),
        check("timeouts and connection resets retry", [C(TimeoutError()), C(ConnectionResetError())], [backfill.RETRY, backfill.RETRY]),
        check("truncated download errors retry", [C(EOFError()), C(gzip.BadGzipFile())], [backfill.RETRY, backfill.RETRY]),
        check("URLError retries", C(urllib.error.URLError("x")), backfill.RETRY),
        check("unexpected error fails without retry", C(ValueError("x")), backfill.FAIL),
        check("a local file error fails without retry", C(FileNotFoundError("x")), backfill.FAIL),
        check("write_marker copies to an s3:// path too", (lambda fc: (backfill.write_marker(fc, "s3://bucket/m/status.json", {"dt": "2026-09-01", "hour": 5, "status": backfill.INGESTED}), any("COPY" in s and "s3://bucket/m/status.json" in s for s in fc.sql))[-1])(FakeCon()), True),
        check("write_marker creates no local directory for an s3:// path", Path("s3:").exists(), False),
        check("marker path zero-pads the hour", backfill.marker_path("s3://ghpulse/_manifest/events", "2026-09-01", 5), "s3://ghpulse/_manifest/events/dt=2026-09-01/hour=05/status.json"),
        check("read_state with no markers is empty", backfill.read_state(con, str(tmp / "manifest_empty")), {}),
        check("a rewritten marker reads back with its latest status", (backfill.write_marker(con, backfill.marker_path(str(tmp / "manifest"), "2026-09-01", 5), {"dt": "2026-09-01", "hour": 5, "status": backfill.FAILED, "rows": None, "source_lines": None, "attempts": 4, "error": "timeout"}), backfill.write_marker(con, backfill.marker_path(str(tmp / "manifest"), "2026-09-01", 5), {"dt": "2026-09-01", "hour": 5, "status": backfill.INGESTED, "rows": 51582, "source_lines": 51582, "attempts": 1, "error": None}), backfill.write_marker(con, backfill.marker_path(str(tmp / "manifest"), "2026-09-01", 6), {"dt": "2026-09-01", "hour": 6, "status": backfill.ABSENT, "rows": None, "source_lines": None, "attempts": 1, "error": None}), backfill.read_state(con, str(tmp / "manifest")))[-1], {("2026-09-01", 5): backfill.INGESTED, ("2026-09-01", 6): backfill.ABSENT}),
        check("a marker round-trips its counts", {k: v for k, v in (backfill.read_marker(con, backfill.marker_path(str(tmp / "manifest"), "2026-09-01", 5)) or {}).items() if k in ("status", "rows", "source_lines", "attempts")}, {"status": backfill.INGESTED, "rows": 51582, "source_lines": 51582, "attempts": 1}),
        check("decompress writes the plain file and counts its lines", [backfill.decompress(make_gz(tmp / "d1.gz", b"line\n" * 200), tmp / "d1.json"), (tmp / "d1.json").exists() and (tmp / "d1.json").read_bytes() == b"line\n" * 200], [200, True]),
        check("decompress counts a final line with no trailing newline", backfill.decompress(make_gz(tmp / "d2.gz", b"a\nb"), tmp / "d2.json"), 2),
        check("a truncated gzip raises an error that retries", raises((EOFError, gzip.BadGzipFile, zlib.error), lambda: backfill.decompress(make_gz(tmp / "d3.gz", b"line\n" * 200, cut=True), tmp / "d3.json")), True),
        check("download writes the bytes and returns the count", [backfill.download("https://example.invalid/x", tmp / "dl_ok", "ghpulse-test", min_bytes=1000, opener=fake_opener(b"x" * 2000, 2000)), (tmp / "dl_ok").stat().st_size if (tmp / "dl_ok").exists() else None], [2000, 2000]),
        check("download sends the User-Agent", seen.get("ua"), "ghpulse-test"),
        check("download rejects a short read against Content-Length", raises(backfill.SuspiciousDownload, lambda: backfill.download("https://example.invalid/x", tmp / "dl_short", "ghpulse-test", min_bytes=1000, opener=fake_opener(b"x" * 2000, 3000))), True),
        check("download rejects an implausibly small file", raises(backfill.SuspiciousDownload, lambda: backfill.download("https://example.invalid/x", tmp / "dl_small", "ghpulse-test", min_bytes=1000, opener=fake_opener(b"x" * 500, 500))), True),
        check("a suspicious download retries", C(backfill.SuspiciousDownload("x")), backfill.RETRY),
        check("download works through urllib on a file:// URL", backfill.download(src.as_uri(), tmp / "dl_file", "ghpulse-test", min_bytes=10), src.stat().st_size),
        check("partition_key rejects a path with no dt= segment", raises(ValueError, lambda: backfill.partition_key('s3://b/bronze/events/hour=05/data.parquet')), True),
        check("partition_key rejects a path with no hour= segment", raises(ValueError, lambda: backfill.partition_key('s3://b/bronze/events/dt=2026-09-01/data.parquet')), True),
        check("partition_key rejects a non-numeric hour", raises(ValueError, lambda: backfill.partition_key('s3://b/bronze/events/dt=2026-09-01/hour=xx/data.parquet')), True),
        check("hour_range with end before start raises ValueError (checked without the placeholder)", raises(ValueError, lambda: backfill.hour_range(backfill.parse_hour('2026-09-02T01'), backfill.parse_hour('2026-09-01T23'))), True),
        check("incomplete reads retry", C(http.client.IncompleteRead(b"")), backfill.RETRY),
        check("corrupt gzip stream errors retry", C(zlib.error("x")), backfill.RETRY),
    ]

    print()
    if all(results):
        print("all checks passed")
        return 0
    print("FAILURES — the transform or schema handling has regressed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
