-- queries.sql — what happened on GitHub in one hour.
--
--   s3env && duckdb -init analysis/init.sql
--   then:  .read analysis/queries.sql
--
-- Run from the repo root, or the relative path will not resolve.

-- ── 1. Did the data land? ───────────────────────────────────────────────────
SELECT
    dt,
    hour,
    count(*)                        AS events,
    count(DISTINCT actor.login)     AS actors,
    count(DISTINCT repo.name)       AS repos,
    min(created_at)                 AS first_event,
    max(created_at)                 AS last_event
FROM events
GROUP BY dt, hour
ORDER BY dt, hour;

-- ── 2. What kind of activity is GitHub, really? ─────────────────────────────
-- Expect PushEvent to be ~95% of everything. Actual collaboration — pull
-- requests, reviews, issues — is a thin slice. That is a finding, not a bug,
-- and it is the first reason to doubt "GitHub activity" as a proxy for
-- "contribution".
SELECT
    type,
    count(*)                                           AS events,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM events
GROUP BY type
ORDER BY events DESC;

-- ── 3. Busiest repositories ─────────────────────────────────────────────────
SELECT
    repo.name                    AS repo,
    count(*)                     AS events,
    count(DISTINCT actor.login)  AS actors,
    count(DISTINCT type)         AS event_types
FROM events
GROUP BY repo.name
HAVING count(DISTINCT actor.login) > 1   -- drop single-actor bot churn
ORDER BY events DESC
LIMIT 20;

-- ── 4. Pull request activity ────────────────────────────────────────────────
-- payload is JSON text in bronze, so fields are reached with ->> and a JSON
-- path. Stable regardless of which event type wrote the row: an absent path
-- yields NULL rather than a schema error.
SELECT
    payload->>'$.action'                                                 AS action,
    count(*)                                                             AS prs,
    count(DISTINCT repo.name)                                            AS repos,
    round(avg(TRY_CAST(payload->>'$.pull_request.additions' AS BIGINT)), 1) AS avg_additions,
    round(avg(TRY_CAST(payload->>'$.pull_request.deletions' AS BIGINT)), 1) AS avg_deletions
FROM events
WHERE type = 'PullRequestEvent'
GROUP BY payload->>'$.action'
ORDER BY prs DESC;

-- ── 5. A first look at the funnel ───────────────────────────────────────────
-- Actors bucketed by the furthest stage they reached. In one hour this is
-- mostly noise: nobody becomes a maintainer in an hour, and the counts are
-- dominated by people who pushed to their own repos. The point is that the
-- shape is right — it becomes meaningful across months, which is what
-- milestone 1 is for.
--
-- The bot filter only catches the '[bot]' suffix convention. Plenty of
-- automation does not follow it, so treat these numbers as an upper bound
-- on humans.
WITH actor_activity AS (
    SELECT
        actor.login AS actor,
        count(*) FILTER (WHERE type = 'WatchEvent')             AS stars,
        count(*) FILTER (WHERE type = 'IssuesEvent')            AS issues,
        count(*) FILTER (WHERE type = 'PullRequestEvent')       AS prs,
        count(*) FILTER (WHERE type = 'PullRequestReviewEvent') AS reviews
    FROM events
    WHERE actor.login NOT LIKE '%[bot]'
    GROUP BY actor.login
)
SELECT
    CASE
        WHEN reviews > 0 THEN '4 reviewer'
        WHEN prs     > 0 THEN '3 contributor'
        WHEN issues  > 0 THEN '2 reporter'
        WHEN stars   > 0 THEN '1 observer'
        ELSE                  '0 pusher only'
    END      AS stage,
    count(*) AS actors
FROM actor_activity
GROUP BY stage
ORDER BY stage;

-- ── 6. How much of this is automation? ──────────────────────────────────────
-- Worth knowing before drawing any conclusion about human behaviour.
SELECT
    CASE WHEN actor.login LIKE '%[bot]' THEN 'bot' ELSE 'human (probably)' END AS kind,
    count(*)                    AS events,
    count(DISTINCT actor.login) AS actors
FROM events
GROUP BY 1
ORDER BY events DESC;
