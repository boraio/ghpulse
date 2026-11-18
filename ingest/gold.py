"""Gold layer: interaction events, the leak-free training aggregate, test targets and repo funnel facts, built from silver."""

from __future__ import annotations

import argparse
import os
import sys

import backfill
import duckdb
import fetch
import silver

# Interaction weights: assumptions, not measurements (milestone_01_step6.md). The most-popular baseline ignores them.
WEIGHTS = {"star": 1, "issue_opened": 2, "pr_opened": 3, "review": 3, "merge": 4}
# (silver table, condition, kind). A merge's actor is whoever merged, so `merge` records maintaining a repo.
KINDS = (
    ("pull_requests", "action = 'opened'", "pr_opened"),
    ("pull_requests", "action = 'merged'", "merge"),
    ("issues", "action = 'opened'", "issue_opened"),
    ("stars", "TRUE", "star"),
    ("reviews", "action = 'created'", "review"),
)

# Start of the test week (UTC). Training uses only events before it; fixed 2026-11-17 in milestone_01_measurements.md.
CUTOFF = "2026-09-29 00:00:00"

# Automation rule (milestone_01_step6.md, decided 2026-11-18): judged on training events only.
AUTOMATION_MAX_REPOS = 50
AUTOMATION_MAX_PER_HOUR = 30

def events_sql() -> str:
    """SELECT of qualifying interaction events over relations pull_requests, issues, stars, reviews and markers (dt, hour, stable)."""
    parts = [
        f"SELECT s.actor_id, s.actor_login, s.repo_id, s.repo_name, '{kind}' AS kind, {WEIGHTS[kind]} AS weight, "
        f"s.created_at, s.dt, s.hour FROM {table} s JOIN markers m ON s.dt = m.dt AND s.hour = m.hour "
        f"WHERE m.stable AND {cond} AND lower(s.actor_login) <> lower(split_part(s.repo_name, '/', 1))"
        for table, cond, kind in KINDS
    ]
    return " UNION ALL ".join(parts)


def train_sql(events: str = "interaction_events") -> str:
    """(actor, repo) pairs from events before CUTOFF only: summed weight, event count, first and last seen."""
    return (
        "SELECT actor_id, any_value(actor_login) AS actor_login, repo_id, any_value(repo_name) AS repo_name, "
        "sum(weight) AS weight, count(*) AS n_events, min(created_at) AS first_seen, max(created_at) AS last_seen "
        f"FROM {events} WHERE created_at < TIMESTAMP '{CUTOFF}' GROUP BY actor_id, repo_id"
    )

# The fixed window (milestone_01_measurements.md, decided 2026-11-17).
WINDOW = ("2026-09-06T00", "2026-10-05T23")


def automation_sql(events: str = "interaction_events", max_repos: int = AUTOMATION_MAX_REPOS, max_per_hour: int = AUTOMATION_MAX_PER_HOUR) -> str:
    """Actors treated as automation, on events before CUTOFF only: more than max_repos distinct repos or more than max_per_hour events in one hour."""
    return (
        "SELECT actor_id, any_value(actor_login) AS actor_login, count(DISTINCT repo_id) AS repos, max(per_hour) AS peak FROM ("
        "SELECT *, count(*) OVER (PARTITION BY actor_id, dt, hour) AS per_hour "
        f"FROM {events} WHERE created_at < TIMESTAMP '{CUTOFF}') "
        f"GROUP BY actor_id HAVING count(DISTINCT repo_id) > {max_repos} OR max(per_hour) > {max_per_hour}"
    )


def main(argv=None) -> int:
    """Build the gold tables for a range of hours from silver and its markers, write them under --gold-root, print counts."""
    p = argparse.ArgumentParser(description=main.__doc__)
    p.add_argument("--start", default=WINDOW[0], help="first hour (UTC), default: start of the fixed window")
    p.add_argument("--end", default=WINDOW[1], help="last hour, inclusive, default: end of the fixed window")
    p.add_argument("--silver-root", default="s3://ghpulse/silver")
    p.add_argument("--manifest-root", default="s3://ghpulse/_manifest/silver")
    p.add_argument("--gold-root", default="s3://ghpulse/gold")
    args = p.parse_args(argv)
    keys = backfill.hour_range(backfill.parse_hour(args.start), backfill.parse_hour(args.end))
    days = sorted({d for d, _ in keys})
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    if any(r.startswith("s3://") for r in (args.silver_root, args.manifest_root, args.gold_root)):
        fetch.s3_config(con)
    con.execute("CREATE TABLE keys (dt VARCHAR, hour INTEGER)")
    con.executemany("INSERT INTO keys VALUES (?, ?)", keys)
    # dt is read as text so it joins silver's dt; only hours inside [start, end] survive the semi-join.
    marker_files = [f"{args.manifest_root}/dt={d}/hour=*/status.json" for d in days]
    con.execute(
        f"CREATE TABLE markers AS SELECT dt, hour, stable FROM read_json({marker_files}, "
        "columns={'dt': 'VARCHAR', 'hour': 'INTEGER', 'stable': 'BOOLEAN'}, hive_partitioning=false) "
        "SEMI JOIN keys USING (dt, hour)"
    )
    for t in silver.TABLES:
        files = [f"{args.silver_root}/{t}/dt={d}/hour=*/data.parquet" for d in days]
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet({files}, hive_partitioning=false)")
    con.execute(f"CREATE TABLE interaction_events AS {events_sql()}")
    outputs = {"interaction_events": "SELECT * FROM interaction_events", "interactions_train": train_sql(),
               "test_targets": test_targets_sql(), "repo_funnel": repo_funnel_sql()}
    for name, sql in outputs.items():
        path = f"{args.gold_root}/{name}/data.parquet"
        if not path.startswith("s3://"):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
        n = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0]
        print(f"{name}: {n:,} rows -> {path}")
    return 0


def test_targets_sql(events: str = "interaction_events") -> str:
    """(actor, repo) pairs first seen at or after CUTOFF, for actors with at least one event before it."""
    return (
        "SELECT actor_id, any_value(actor_login) AS actor_login, repo_id, any_value(repo_name) AS repo_name, "
        f"min(created_at) AS first_seen FROM {events} GROUP BY actor_id, repo_id "
        f"HAVING min(created_at) >= TIMESTAMP '{CUTOFF}' "
        f"AND actor_id IN (SELECT actor_id FROM {events} WHERE created_at < TIMESTAMP '{CUTOFF}')"
    )


def repo_funnel_sql(events: str = "interaction_events") -> str:
    """Per repo, distinct outside actors at each stage: stargazers, issue openers, PR openers, reviewers, mergers."""
    stages = ", ".join(
        f"count(DISTINCT actor_id) FILTER (WHERE kind = '{kind}') AS {name}"
        for kind, name in (("star", "stargazers"), ("issue_opened", "issue_openers"), ("pr_opened", "pr_openers"),
                           ("review", "reviewers"), ("merge", "mergers"))
    )
    return (
        f"SELECT repo_id, any_value(repo_name) AS repo_name, {stages}, count(DISTINCT actor_id) AS actors "
        f"FROM {events} GROUP BY repo_id"
    )


if __name__ == "__main__":
    sys.exit(main())
