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
    return "SELECT 1 AS x"  # stub: wrong on purpose, so every check fails before the implementation
