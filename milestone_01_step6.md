# Milestone 1, step 6: gold (decisions, progress, open items)

Status as of 2026-11-18: **built, automation rule applied, run live.** Branch `gold`
(not yet merged), 155 PASS, ruff clean.

## Built (test-first)

- `ingest/gold.py`: `WEIGHTS`, `KINDS`, `CUTOFF`, `events_sql()`, `train_sql()`,
  `test_targets_sql()`, `repo_funnel_sql()`, and a runner (`python
  ingest/gold.py`, window by default) that writes four Parquet tables under
  `s3://ghpulse/gold/`. Checks against `tests/gold_fixtures.py` (one row per
  rule, including a leak trap: alice's training weight for `org/a` must stay 3,
  not 4, because her star on it falls in the test week) and two script-run
  checks via `tests/gold_runner_fixtures.py` (stable hour: 6 events, 4 training
  pairs, 0 targets; unstable hour: 0 events).
- aider incidents: a stub nested inside the previous function (indented to match
  its anchor; dedented by a guarded script, `091cb19`), and the runner block
  placed mid-file so a script run hit `main()` before later functions existed
  (moved by a guarded script, `a2bc367`; caught by the script-run check). The red
  checkpoint for batch B was not observed before its implementation.

## First live run (2026-11-17, 13 s)

| Table | Rows |
|---|---|
| `interaction_events` | 933,860 (star 312,704; pr_opened 200,485; review 176,545; merge 164,027; issue_opened 80,099) |
| `interactions_train` | 441,295 pairs; 327,785 actors; 219,669 repos |
| `test_targets` | 41,612 pairs; **25,310 evaluable actors**; 32,899 repos |
| `repo_funnel` | 285,548 repos |

Test targets per evaluable actor: median 1, p90 2, **max 3,310** (one actor:
almost certainly automation without a `[bot]` suffix; to be looked at before
step 7, since it would dominate any pooled metric).

**Popularity concentration of the test targets**, by the repos' training
popularity (distinct training actors): 1.4% fall on the top 10 repos, 6.6% on
the top 100, 20.3% on the top 1,000, and **58.9% on repos never seen in
training**. So no recommender that ranks training repos, most-popular included,
can hit more than about 41% of the targets; that ceiling is reported with
step 7's numbers. Top training repos (alibaba/open-code-review,
bilawalsidhu/gods-eye-view, deepseek-ai/deepseek-harness, …) look like
star-driven trending repos.

## Automation rule (decided 2026-11-18, before any scoring)

The heaviest actors: two `Copilot` accounts (GitHub's coding agent, no `[bot]`
suffix; one reviewed PRs on 12,140 repos and held 3,310 test targets, the two
together about 10% of all targets), release and dependency bots
(`release-service-bot`, `regro-cf-autotick-bot`, `pyup-bot`, `scala-steward`,
…), and star bursts (`napzak5`: 369 repos in 3 hours, 214 in one hour). A login
regex was rejected: "bot" anywhere matches 2,536 logins, many of them human.

**Rule:** an actor is treated as automation if, **in the training period only**
(events before 2026-09-29), it touched **more than 50 distinct repos** (training
p99.9 is 17, p99.99 is 72) **or had more than 30 events in one hour** (p99.99 is
29). Measuring on training data only keeps the test week from deciding who is
evaluated. Excluded actors are left out of `interactions_train`,
`test_targets` and `repo_funnel`; `interaction_events` stays complete and the
list is written as `gold/automation_actors`.

Sensitivity (training-only measurements):

| Rule | Actors excluded | Training pairs removed | Test targets removed | Evaluable actors left |
|---|---|---|---|---|
| repos > 50 or peak > 20 | 121 | 4.04% | 12.33% | 25,259 |
| **repos > 50 or peak > 30 (chosen)** | **81** | **3.91%** | **12.25%** | **25,270** |
| repos > 50 or peak > 50 | 75 | 3.88% | 12.23% | 25,272 |
| repos > 100 or peak > 50 | 31 | 3.23% | 11.04% | 25,298 |

The choice of threshold barely moves anything; most of the removed targets
belong to Copilot. Actors caught only by the burst test are mostly automation
working one repo (`alembic-fleet-bot`, `biotools-bot`, `BrewTestBot`, accounts
with ~100 events on someone else's repo in an hour) and a few small star farms
(34–41 repos, peaks of 38–45).

**Built and rerun (2026-11-18):** `automation_sql(events, max_repos, max_per_hour)`
with the thresholds as constants; the runner writes `automation_actors` and
builds training pairs, targets and the funnel from `kept_events`
(`interaction_events` minus those actors). Seven checks, including the
training-only behaviour (alice's second repo and her one-hour burst both fall in
the test week, so only bob is flagged). Suite at 155 PASS.

| Table | Before the rule | After |
|---|---|---|
| `automation_actors` | n/a | 81 (two `Copilot` accounts with 8,830 and 1,800 training repos, `release-service-bot`, `napzak5`, `Corykidios`, `ORESoftware`, …) |
| `interactions_train` | 441,295 pairs, 327,785 actors, 219,669 repos | 424,020 pairs, 327,704 actors, 207,490 repos |
| `test_targets` | 41,612 pairs, 25,310 actors, 32,899 repos | **36,516 pairs, 25,270 evaluable actors, 28,285 repos** |
| targets per actor (median, p90, p99, max) | 1, 2, ?, 3,310 | 1, 2, 7, 242 |
| targets on training top 10 / 100 / 1,000 / unseen | 1.4% / 6.6% / 20.3% / 58.9% | 1.5% / 7.0% / 21.4% / **56.3%** |

No excluded actor remains in training or targets. The 242-target actor was not
caught because the rule looks only at training behaviour; metrics are averaged
per actor, so it counts once. **Reachability ceiling for any recommender that
ranks training repos: about 43.7% of targets.**

## Next

- Docs sync, PR and merge for `gold`.
- Step 7: most-popular on this split, Recall@10 and NDCG@10, with catalogue
  coverage, the unreachable-target ceiling and the masked-hour share reported
  alongside.

Gold is built in DuckDB from silver, over the fixed window 2026-09-06T00 …
2026-10-05T23 (train Sep 6–28, test week Sep 29 – Oct 5; see
`milestone_01_measurements.md`).

## Decisions (agreed 2026-11-17)

1. **What counts as an interaction** (`kind`, from silver `action`):

   | Kind | Source | Meaning |
   |---|---|---|
   | `star` | `stars` (every row) | starred the repo |
   | `issue_opened` | `issues`, `action = 'opened'` | opened an issue |
   | `pr_opened` | `pull_requests`, `action = 'opened'` | opened a pull request |
   | `review` | `reviews`, `action = 'created'` | submitted a review |
   | `merge` | `pull_requests`, `action = 'merged'` | merged a PR: the actor is whoever merged, so this records maintaining the repo, not having a PR accepted (the author is not in the payload) |

   Excluded as housekeeping: labeled, unlabeled, assigned, unassigned, closed,
   reopened, review updated, review dismissed (about 25% of issue events and 11%
   of PR events in stable hours).
2. **Own-repo activity is excluded** (actor login equals the repo owner,
   case-insensitive) from interactions and funnel facts; its share is reported.
   In stable hours it is 61% of PR events, 56% of issue events, 20% of reviews,
   6% of stars.
3. **Weights are named constants and assumptions:** star 1, issue_opened 2,
   pr_opened 3, review 3, merge 4. The most-popular baseline does not use them.
   A pair's training weight is the sum of its events' weights.
4. **Stable hours only**, via the silver markers' `stable` flag.
5. **Leak-free layout:**
   - `gold/interaction_events`: one row per qualifying event (actor, repo, kind,
     weight, created_at, dt, hour).
   - `gold/interactions_train`: one row per (actor, repo) from events **before
     2026-09-29 00:00 UTC only**: summed weight, event count, first and last
     seen.
   - `gold/test_targets`: (actor, repo) pairs first seen in the test week, for
     actors with at least one training pair: the "evaluable" pairs.
6. **`gold/repo_funnel`:** per repo, distinct outside actors at each stage
   (stargazers, issue openers, PR openers, reviewers, mergers). Capture may
   differ by event type, so cross-stage ratios carry that caveat.

## Order of work (test-first, with aider)

A. `events_sql()` over relations `pull_requests`, `issues`, `stars`, `reviews`,
   `markers`, checked against `tests/gold_fixtures.py`, where each fixture row
   exercises one inclusion or exclusion rule.
B. `train_sql()` and `test_targets_sql()` with the cutoff as a constant.
C. `repo_funnel_sql()`.
D. A runner that loads silver and markers for the window, writes the four gold
   tables to `s3://ghpulse/gold/…`, and reports counts; live run and checks.

## Known limitations carried into evaluation

- About 2% capture of PR activity; test targets are themselves a sample.
- Silver activity per stable hour swings about 12× by day; the test week runs
  about 2.3× the Sep 22–28 rate.
- Bots without the `[bot]` suffix remain in the data.
