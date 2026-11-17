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
