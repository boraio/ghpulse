# Milestone 1, step 0 — backfill mechanics: decisions, progress, open items

Status as of 2026-10-19 (evening): **in progress.** Branch `backfill-mechanics`,
local and unpushed. No backfill has been run. This page is the working log for
step 0 of `milestone_01.md`; fold what matters into the README and retire it
when step 0 merges.

## Agreed design

**Manifest.** One small JSON marker per partition, at
`s3://ghpulse/_manifest/events/dt=<date>/hour=<HH>/status.json`. Status is one of
`ingested`, `absent` or `failed`; no marker means never attempted. The data file
is written before its marker, so a crash can only leave data without a marker,
which resume simply redoes. An `ingested` marker with no data behind it is
reported as drift by `--status`. The marker records `rows` and `source_lines`
side by side.

**Resume semantics.** Default run: skip `ingested`, retry `failed`, skip
`absent`, redo anything with data but no marker, and redo an `ingested` marker
whose data is missing. `--retry-absent` also retries absent hours. `--force`
redoes everything and needs `--yes`. `--dry-run` shows the plan. `--status`
reports without changing anything. The range is `--start 2026-09-01T00 --end
2026-09-07T23`, inclusive, in UTC hours.

**Failure handling** (`classify`):

| Error | Outcome |
|---|---|
| HTTP 404 | recorded `absent` |
| HTTP 401, 403 | abort the whole run |
| HTTP 429 or any 5xx | retry |
| other HTTP 4xx | fail, no retry |
| URLError, timeouts, connection errors, `http.client` errors, `EOFError`, bad or corrupt gzip | retry |
| anything else (including local file errors and transform errors) | fail, no retry |

`HTTPError` is checked before `URLError` because it is a subclass of it; a check
guards the order. Retry waits (`backoff_s`): 2 s base, doubling per attempt,
capped at 60 s, with jitter between 0.5x and 1x the delay; a server's
`Retry-After` can lengthen the wait but never shorten it. Four attempts per hour
(applied in the worker, not yet written). The run exits non-zero if any hour
failed, and warns if more than 3 hours came back absent.

**Parallelism.** A spawn-based process pool, default 4 workers, with each
worker's DuckDB thread count set to cores divided by workers. To be piloted: 6
hours at 1 worker, then at 4, before the full week.

**Publish lag guard** (`too_recent`): refuse an hour whose start is less than
3 hours before now. This number is a guess (see open items). It does not apply
to the September week, which is six weeks old.

## Done and verified

- `ingest/fetch.py` refactor (`gharchive_url`, `bronze_path`, `transform_sql`)
  with no behaviour change. Verified by dry-run output, a rendered-SQL
  comparison (identical ignoring indentation), and a Parquet comparison for
  2026-09-01 hour 0 against a baseline copy: same schema, 0 differing rows
  either way, 51,582 rows. A control run on identical input passed first, so the
  harness itself is trusted.
- `python ingest/fetch.py 2026-09-14 12` still behaves as before.
- `ingest/backfill.py`: `parse_hour` (tz-aware UTC), `hour_range` (inclusive,
  crosses midnight), `partition_key`, `plan`, `classify`, `count_lines`,
  `backoff_s`, `too_recent`. All covered in `tests/test_transform.py`, each
  written test-first and seen to fail first. `count_lines` was also probed
  outside the suite with empty chunks, a blank line and generator input.
- `backfill.py` tidied: imports moved to the top, a one-line module docstring
  added above `from __future__`, and `partition_key` now raises `ValueError`
  (not `StopIteration`) for a path with no `dt=` or `hour=` segment. Driven by a
  new `raises(exc_type, fn)` helper in the test file and checks that failed
  first.
- Suite: **45 PASS, 0 FAIL.**
- Test file only ever gains lines. One incident: a model edit deleted the nine
  classifier checks and the suite stayed green; the removed-lines guard caught it
  and the file was restored from the known-good commit.
- aider's temperature setting verified end to end (see `PROJECT.md`, aider
  notes): now `0.0`, confirmed arriving at the server.
- Draft PR #1 open; CI green with pinned ruff 0.17.1 (its default rules, via `ruff.toml` and `requirements-dev.txt`), `compileall`, and the offline checks now running in CI.

## Remaining, in order

1. **Side-effect pieces**, each with offline checks: download (timeout,
   `Content-Length` check, minimum-size check, a `SuspiciousDownload` error),
   decompress to disk returning the line count via `count_lines`, read state
   (listing markers), write marker (local temp file, then DuckDB `COPY ... TO
   ... (FORMAT JSON)`), the per-hour worker `ingest_one` (retry loop, marker
   written after data), and a `code_version` stamp in the marker. When these
   land, reword the module docstring, which currently says "No I/O".
2. **CLI**: `--start`, `--end`, `--workers`, `--retry-absent`, `--force --yes`,
   `--dry-run`, `--status`.
3. **Pilot and run.** 6 hours at 1 worker then 4 and compare throughput. Then
   `--status` and `--dry-run` for the full week. Then
   `2026-09-01T00` to `2026-09-07T23`. Hour 0 of 2026-09-01 already exists
   without a marker and will be redone harmlessly.
4. **Measurements** (steps 1 and 2 of `milestone_01.md`): duplicate `id` rate
   within an hour, a day and the week (read only the `id` column, to avoid the
   grouped `count(DISTINCT)` crash); bot share of events and of distinct actors
   by the `[bot]` suffix; Parquet bytes per hour and per day. Use
   `hive_partitioning=false` on every bronze read.

## Open items and unknowns

- **A vacuous existing check.** "hour_range with end before start raises
  ValueError" is built as `check(..., True, True)`, which prints PASS at once; a
  later `try` block only flips `results[-1]` to `False`. If `hour_range` stopped
  raising, the output would still say PASS and the suite would end in
  "FAILURES" with no FAIL line, so the FAIL-count guard would read 0. Read from
  the code, not mutation-tested. A clean duplicate using `raises()` now sits
  beside it. Fixing or removing the original conflicts with "never restructure
  existing checks", so it is the owner's decision.
- **GH Archive's publish lag is unmeasured.** The 3-hour guard is a conservative
  guess. If it is too short, the cost is a 404 recorded as `absent`, which
  `--retry-absent` redoes.
- **GH Archive rate limits are unknown.** The default of 4 workers is a starting
  point for the pilot, not a finding.
- **Blank lines in source files.** `count_lines` counts them as lines. How
  DuckDB treats them is unknown. If `source_lines` and `rows` disagree on real
  data, look here first.
- **`ignore_errors=true` and silent drops.** `source_lines` is recorded beside
  `rows` as a precaution against malformed lines being dropped without notice.
  Whether it ever happens on this data has not been observed.
- **`duckdb.IOException` classification.** Planned as a retry, but it is not
  known whether DuckDB raises it only for S3 problems or also for local read
  errors. Needs a run against a real failure; until then only download-side
  errors are classified.
- **Bucket versioning** looked off (empty output) but was never confirmed with
  `--output json; echo $?`. Matters for overwrite-based reprocessing.
- **One-hour measurements so far are single readings**: 2026-09-01 hour 0 is
  51,582 events and about 5.9 MB of Parquet; a single fetch took about 3.6 s
  wall with a peak of about 370 MB resident, on a 32-core, 440 GB host. The
  step 2 measurement supersedes the size figure.
- **Branch history** is many small aider commits, one commit with a syntax error
  that the next commit fixed, and a restore commit. Squash before merging. That
  rewrites history, so it is a deliberate step, taken before anything is pushed.
- **Repo copies of the docs.** The repo tracks its own `PROJECT.md` and
  `milestone_01.md`; the copies kept outside the repo were updated first. Whether the
  two were identical before is not known, so review `git diff` before
  committing any sync.

## Step 0 is done when

- Interrupting the backfill and re-running completes the remainder without
  reprocessing what landed.
- `--status` shows ingested, absent, failed and drift hours distinctly.
- The offline checks cover `plan`, `classify`, the counters and the guards, and
  CI is green on the merged branch.
- A pilot has set the worker count from a measurement.
