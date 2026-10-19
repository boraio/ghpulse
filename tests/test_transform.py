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

    print()
    if all(results):
        print("all checks passed")
        return 0
    print("FAILURES — the transform or schema handling has regressed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
