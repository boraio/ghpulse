"""Gold layer: interaction events, the leak-free training aggregate, test targets and repo funnel facts, built from silver."""

from __future__ import annotations

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
    return "SELECT 1 AS x"  # stub: train_sql


def test_targets_sql(events: str = "interaction_events") -> str:
    """(actor, repo) pairs first seen at or after CUTOFF, for actors with at least one event before it."""
    return "SELECT 1 AS x"  # stub: test_targets_sql
