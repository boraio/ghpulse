# Milestone 1, step 3: silver layer (decisions, progress, open items)

Status as of 2026-11-10: **runner complete and verified live** (`silver_one`, `silver_todo`, CLI).
Branch `silver` (history rewritten and force-pushed 2026-11-10), 113 PASS, ruff clean.
**Silver built for Sep 22–28 and reconciled with bronze** (2026-11-12). Next: sync the
repo docs, PR and squash-merge, then step 4.

Done: `ingest/silver.py` with `silver_sql(table)` for the four tables, driven by
10 offline checks written first and seen failing against a stub (suite at 88
PASS). The ISC004 lint fix was applied with `ruff --fix --unsafe-fixes`, not
aider (a mechanical six-line change ruff previewed exactly). Live on 2026-09-23
hour 12: for every table, bronze rows minus `[bot]` rows minus duplicates
equals silver rows (PRs 1,223 − 386 − 0 = 837; issues 569 − 74 − 0 = 495; stars
235 − 0 − 0 = 235; reviews 313 − 108 − 0 = 205); no null actor logins; no
column null, `from_fork` included. One hour, so a single reading: `[bot]`
accounts made 32% of PR events and 35% of review events in it. (`SUMMARIZE`'s
`approx_unique` is an estimate and can exceed the row count.)

`is_stable(n, push)` added the same way: 11 checks written first and seen failing
against a stub returning `None` (88 PASS, 11 FAIL), then the implementation (99
PASS). The checks pin the bounds as inclusive, matching the `BETWEEN` queries
the measurements used. `REF_NONPUSH = 16893` lives in `silver.py`.

`silver_one(con, key, *, bronze_root, silver_root, manifest_root,
code_version)` (2026-11-10): loads one bronze hour into a temp table, writes the
four tables to `<silver_root>/<table>/dt=…/hour=HH/data.parquet` (reusing
`backfill.bronze_file`), then writes the marker last. The marker holds
`bronze_rows`, `push`, `stable`, and per table `_in`, `_bots`, `_dups`,
`_rows`. Nine checks against a synthetic bronze file in a temp dir, seen
failing first (99 PASS, 9 FAIL), then 108 PASS. Live smoke test on 2026-09-23
hour 12 into `s3://ghpulse/_scratch/silver-test`: 1.4 s for the hour; record,
marker read back from Ceph, and the Parquet files all give 837 / 495 / 235 /
205 rows, matching the hand reconciliation; `stable` true.

**Decision: no worker pool for silver.** At about 1.4 s per hour, Sep 22–28
is about 4 minutes and all 1,248 bronze hours about 30 minutes, run serially.
A serial loop also avoids the pool's Ctrl-C problem. Revisit only if a measured
run is too slow.

`silver_todo(keys, silver_statuses, silver_present, bronze_statuses, mode)`:
`backfill.plan()` over the silver manifest, limited to hours whose bronze is
`ingested` (absent or missing bronze is skipped, not failed). Five checks, red
then green (113 PASS). CLI `python ingest/silver.py --start … --end …` with
`--status`, `--dry-run`, `--force --yes` and root overrides; a serial loop; an
exception in one hour writes a `failed` marker and the run continues. Verified
live: `--status` on Sep 22–28 showed 168 unattempted and 0 without bronze;
`--dry-run` planned 168; `--force` without `--yes` was refused; on the scratch
prefix, Sep 23 hours 10–12 planned 2 of 3 (hour 12 already built by the smoke
test), built them, and a rerun planned 0 of 3. Two lint fixes were applied with
`ruff --fix` (ISC004 earlier, I001 here: ruff treats `backfill` and `fetch` as
third-party). aider hit its 16,384-token context with both files and the repo
map open; prompts now open only the file being edited, with `--map-tokens 0`.

This page is the working log for step 3 of `milestone_01.md`, the same way
`milestone_01_step0.md` was for the backfill.

## First real run: Sep 22–28 (2026-11-12)

`python -u ingest/silver.py --start 2026-09-22T00 --end 2026-09-28T23`: 168
hours, 0 failed, 3 min 39 s (about 1.3 s per hour). `--status`: 168 ingested,
0 drift. Reconciliation over the 168 markers and the stored files:

| Table | Bronze in | `[bot]` | Duplicates | Silver rows | Hours not reconciling | Rows on storage | Duplicate ids across the week | `[bot]` share |
|---|---|---|---|---|---|---|---|---|
| pull_requests | 238,024 | 84,427 | 0 | 153,597 | 0 | 153,597 | 0 | 35.5% |
| issues | 93,684 | 15,070 | 0 | 78,614 | 0 | 78,614 | 0 | 16.1% |
| stars | 40,467 | 0 | 1 | 40,466 | 0 | 40,466 | 0 | 0.0% |
| reviews | 55,325 | 19,500 | 2 | 35,823 | 0 | 35,823 | 0 | 35.2% |

Every silver marker's `bronze_rows` equals the bronze marker's `rows` (0
mismatches), all 168 hours are `stable`, and per-hour dedup leaves no duplicate
id anywhere in the week. So the exit criterion "bronze → silver row counts
reconcile, with any discrepancy explained by the dedup and bot filters" holds
for this week. Self-declared bots are about a third of PR and review events and
none of the stars.

## Decisions already made

- **Engine: DuckDB**, not Spark (`PROJECT.md`, revised 2026-10-23). Silver is a
  per-hour scan, filter and dedup, so it runs hour by hour like the bronze
  backfill.
- **Partitioning mirrors bronze:**
  `s3://ghpulse/silver/<table>/dt=<date>/hour=<HH>/data.parquet`, so every
  silver hour traces to exactly one bronze hour.
- **Dedup on `id` within the hour.** Measured: 5 duplicates in 14.9M rows, all
  within single hours. A cross-hour uniqueness check stays as a data-quality
  gate (step 4).
- **Bot filter: drop actors whose login ends in `[bot]`.** A lower bound;
  unsuffixed bots are an open item.
- **Nothing is filtered by the stability rule.** Each silver hour records a
  `stable` flag; gold and evaluation apply it.
- **All actions are kept** (opened, merged, closed, labeled, …) with an
  `action` column. Which actions count as interactions, and with what weight,
  is a gold decision.

## Tables

Common columns on every table: `id`, `created_at`, `actor_id`, `actor_login`,
`repo_id`, `repo_name`, `dt`, `hour`.

| Table | Source type | Extra columns (JSON path in `payload`) |
|---|---|---|
| `pull_requests` | `PullRequestEvent` | `action` (`$.action`), `pr_id` (`$.pull_request.id`), `pr_number` (`$.pull_request.number`), `head_repo_id` (`$.pull_request.head.repo.id`), `base_repo_id` (`$.pull_request.base.repo.id`), `from_fork` (head ≠ base; NULL when either is missing) |
| `issues` | `IssuesEvent` | `action`, `issue_id` (`$.issue.id`), `issue_number` (`$.issue.number`), `issue_author` (`$.issue.user.login`), `title` (`$.issue.title`) |
| `stars` | `WatchEvent` | none (action is always `started`) |
| `reviews` | `PullRequestReviewEvent` | `action`, `pr_number` (`$.pull_request.number`), `review_id` (`$.review.id`), `state` (`$.review.state`) |

The planned PR columns `additions`, `deletions` and `merged` do not exist in
current payloads (GitHub Events API change, effective 2025-10-07). A merge is
an event with `action = 'merged'`, and its actor is whoever merged, not the
author; the author is not in the payload.

## Per-hour marker

`s3://ghpulse/_manifest/silver/dt=<date>/hour=<HH>/status.json`, written after
the four data files, recording: bronze rows in, rows per table out, duplicates
removed, bot rows removed, and `stable`. Bronze → silver reconciliation (an
exit criterion) is computed from these markers.

## Order of work (test-first, with aider)

1. `silver_sql(table)` returning the SELECT for one table over a relation named
   `bronze`; offline checks on synthetic rows: columns and values per table,
   dedup on `id`, `[bot]` dropped, `from_fork` true/false/NULL.
2. **Next: per-hour runner** `silver_one(con, key, ...)` plus a CLI, reusing
   from `backfill.py`: `marker_path(root, dt_iso, hour)`, `write_marker(con,
   path, record)` (local temp JSON, then `COPY` to the target), `read_state(con,
   root)` (reads only `dt`, `hour`, `status`), `plan(keys, statuses, present,
   mode)`, `status_counts`, `exit_code`, `hour_range`, `parse_hour`.
   `run_pool` is not reusable as is: it is wired to `_worker` → `ingest_one`.
   Options: a silver pool of its own (new code, no change to tested backfill
   code) or generalising `run_pool` to take the worker function (a refactor of
   tested code). Leaning to the first. Either way the known Ctrl-C issue
   applies. Per hour: load the bronze hour into a temp table `bronze`, `COPY`
   each `silver_sql(t)` to its silver path, then write the marker with bronze
   rows, per-table rows, duplicates and bot rows removed, `n`, `push` and
   `is_stable(n, push)`.
3. Live run on Sep 22–28, then reconcile against bronze.

## Open items

- Unsuffixed bots (event-rate and naming heuristics).
- Whether `IssuesEvent` ever carries pull requests (`$.issue.pull_request`).
- `head.repo` can be null when a fork is deleted; how often is unmeasured.
