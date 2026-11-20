"""Tiny silver tables and markers for the gold checks: each row exercises one inclusion or exclusion rule."""

from __future__ import annotations

import duckdb

COLS = "id VARCHAR, created_at TIMESTAMP, actor_id BIGINT, actor_login VARCHAR, repo_id BIGINT, repo_name VARCHAR, dt VARCHAR, hour INTEGER"
# stars has no action column in silver, so its fixture has none either.
ROWS = {
    "pull_requests": [
        ("p1", "2026-09-20 10:00:00", 1, "alice", 100, "org/a", "2026-09-20", 10, "opened"),
        ("p2", "2026-09-20 10:05:00", 1, "alice", 101, "alice/x", "2026-09-20", 10, "merged"),
        ("p3", "2026-09-20 10:10:00", 2, "bob", 100, "org/a", "2026-09-20", 10, "merged"),
        ("p4", "2026-09-20 10:15:00", 3, "carol", 100, "org/a", "2026-09-20", 10, "labeled"),
        ("p5", "2026-09-20 11:00:00", 4, "dave", 100, "org/a", "2026-09-20", 11, "opened"),
    ],
    "issues": [
        ("i1", "2026-09-30 10:00:00", 1, "alice", 102, "org/b", "2026-09-30", 10, "opened"),
        ("i2", "2026-09-30 10:05:00", 5, "erin", 102, "org/b", "2026-09-30", 10, "opened"),
    ],
    "stars": [
        ("s1", "2026-09-30 10:10:00", 1, "alice", 100, "org/a", "2026-09-30", 10),
        ("s2", "2026-09-20 10:20:00", 6, "frank", 104, "frank/f", "2026-09-20", 10),
    ],
    "reviews": [
        ("r1", "2026-09-20 10:25:00", 2, "bob", 103, "org/c", "2026-09-20", 10, "created"),
        ("r2", "2026-09-20 10:30:00", 2, "bob", 103, "org/c", "2026-09-20", 10, "updated"),
    ],
}
MARKERS = [("2026-09-20", 10, True), ("2026-09-20", 11, False), ("2026-09-30", 10, True)]


def gold_con():
    """A DuckDB connection holding the four silver tables and a markers table."""
    con = duckdb.connect()
    for table, rows in ROWS.items():
        cols = COLS if table == "stars" else COLS + ", action VARCHAR"
        con.execute(f"CREATE TABLE {table} ({cols})")
        con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
    con.execute("CREATE TABLE markers (dt VARCHAR, hour INTEGER, stable BOOLEAN)")
    con.executemany("INSERT INTO markers VALUES (?, ?, ?)", MARKERS)
    return con


def rows(sql: str):
    """Sorted result rows of sql over gold_con(), or an error string, so a broken query fails a check instead of crashing."""
    try:
        return sorted(gold_con().execute(sql).fetchall())
    except Exception as exc:  # noqa: BLE001
        return f"error: {type(exc).__name__}: {exc}"[:200]


def columns(sql: str):
    """Column names of sql over gold_con(), or an error string."""
    try:
        return [d[0] for d in gold_con().execute(sql).description]
    except Exception as exc:  # noqa: BLE001
        return f"error: {type(exc).__name__}: {exc}"[:200]
