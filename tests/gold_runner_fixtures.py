"""Run ingest/gold.py as a script over the silver_one fixture folder, with a stable or an unstable marker."""

from __future__ import annotations

import pathlib
import subprocess
import sys

import backfill
import duckdb

HOUR = ("2026-09-23", 12)


def stable_manifest(root) -> str:
    """Write a marker calling the fixture hour stable under root/manifest_stable; return that manifest root."""
    manifest = str(pathlib.Path(root) / "manifest_stable")
    record = {"dt": HOUR[0], "hour": HOUR[1], "status": "ingested", "stable": True}
    backfill.write_marker(duckdb.connect(), backfill.marker_path(manifest, *HOUR), record)
    return manifest


def run_gold(root, manifest_root: str, gold_name: str) -> int:
    """Run ingest/gold.py as a script on root's silver for the fixture hour, writing under root/gold_name; return its exit code."""
    script = pathlib.Path(__file__).resolve().parent.parent / "ingest" / "gold.py"
    hour = f"{HOUR[0]}T{HOUR[1]:02d}"
    args = ["--start", hour, "--end", hour, "--silver-root", str(pathlib.Path(root) / "silver"),
            "--manifest-root", manifest_root, "--gold-root", str(pathlib.Path(root) / gold_name)]
    return subprocess.run([sys.executable, str(script), *args], capture_output=True).returncode
