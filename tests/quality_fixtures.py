"""Bad silver files for the quality gates: copies of a good file with one deliberate defect each."""

from __future__ import annotations

import pathlib

import duckdb

import backfill

DT = "2026-09-23"


def bad_silver_file(root, name: str, update: str = "", hour: int = 12) -> str:
    """Copy root's good pull_requests file (hour 12), apply an UPDATE to the copy, write it under `hour`; return its path."""
    good = backfill.bronze_file(str(pathlib.Path(root) / "silver" / "pull_requests"), DT, 12)
    dest = backfill.bronze_file(str(pathlib.Path(root) / "bad" / name / "pull_requests"), DT, hour)
    pathlib.Path(dest).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t AS SELECT * FROM read_parquet('{good}')")
    if update:
        con.execute(f"UPDATE t SET {update}")
    con.execute(f"COPY t TO '{dest}' (FORMAT PARQUET)")
    return dest
