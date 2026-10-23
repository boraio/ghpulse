"""Silver layer: typed, deduplicated, bot-filtered tables built from bronze, one per event type."""

from __future__ import annotations

TABLES = ("pull_requests", "issues", "stars", "reviews")

_COMMON = "id, created_at, actor.id AS actor_id, actor.login AS actor_login, repo.id AS repo_id, repo.name AS repo_name, dt, hour"

# Payload fields per table. GitHub's slimmed PR payloads (2025-10-07) carry no author, merged flag or size.
_SPEC = {
    "pull_requests": ("PullRequestEvent", ("json_extract_string(payload, '$.action') AS action, "
                      "CAST(json_extract_string(payload, '$.pull_request.id') AS BIGINT) AS pr_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.number') AS BIGINT) AS pr_number, "
                      "CAST(json_extract_string(payload, '$.pull_request.head.repo.id') AS BIGINT) AS head_repo_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.base.repo.id') AS BIGINT) AS base_repo_id, "
                      "CAST(json_extract_string(payload, '$.pull_request.head.repo.id') AS BIGINT) "
                      "<> CAST(json_extract_string(payload, '$.pull_request.base.repo.id') AS BIGINT) AS from_fork")),
    "issues": ("IssuesEvent", ("json_extract_string(payload, '$.action') AS action, "
               "CAST(json_extract_string(payload, '$.issue.id') AS BIGINT) AS issue_id, "
               "CAST(json_extract_string(payload, '$.issue.number') AS BIGINT) AS issue_number, "
               "json_extract_string(payload, '$.issue.user.login') AS issue_author, "
               "json_extract_string(payload, '$.issue.title') AS title")),
    "stars": ("WatchEvent", ""),
    "reviews": ("PullRequestReviewEvent", ("json_extract_string(payload, '$.action') AS action, "
                "CAST(json_extract_string(payload, '$.pull_request.number') AS BIGINT) AS pr_number, "
                "CAST(json_extract_string(payload, '$.review.id') AS BIGINT) AS review_id, "
                "json_extract_string(payload, '$.review.state') AS state")),
}

# Stability rule from milestone_01_measurements.md: median non-push events per hour in Sep 22-28.
REF_NONPUSH = 16893


def is_stable(n: int, push: int) -> bool:
    """True when an hour looks like normal capture: pushes present, push share 0.50-0.95 and non-push events within 3x of REF_NONPUSH, bounds inclusive."""
    return None  # stub


def silver_sql(table: str) -> str:
    """Return the SELECT that builds one silver table from a relation named `bronze`."""
    if table not in _SPEC:
        raise ValueError(f"unknown silver table: {table!r}")
    event_type, extra = _SPEC[table]
    cols = _COMMON + (", " + extra if extra else "")
    # Dedup on id within one bronze hour: every measured duplicate was within a single hour.
    return (
        f"SELECT {cols} FROM bronze "
        f"WHERE type = '{event_type}' AND actor.login NOT LIKE '%[bot]' "
        "QUALIFY row_number() OVER (PARTITION BY id ORDER BY created_at) = 1"
    )
