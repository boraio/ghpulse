# Milestone 1, step 0 — backfill mechanics: decisions, progress, open items

Status as of 2026-10-23: **functionally complete.** Branch `backfill-mechanics`,
pushed as draft PR #1, CI green, 78 offline checks. The tool has backfilled two
weeks (see `milestone_01_measurements.md`). Remaining: a deliberate
interrupt-and-resume test, and turning off DuckDB's progress bars in workers. This page is the working log for
step 0 of `milestone_01.md`; fold what matters into the README and retire it
when step 0 merges.

**Update, 2026-10-23: step 0 is complete and merged to `main` as `201a19d` (PR #1).**
Interrupt-and-resume test on 2026-09-15…21 (168 hours, 4 workers): stopped with
Ctrl-C after 51 hours had markers. `--status` then showed 51 ingested, 117
unattempted, 0 failed, 0 drift, and `--dry-run` planned exactly 117. The resumed
run ingested those 117 and no others, ending at 168 ingested, 0 failed, 0 drift.
Worker progress bars are off (`SET enable_progress_bar=false`, checked live).
Bronze now covers 2026-09-01…28 contiguously, 672 hours. Known issue, not
fixed: Ctrl-C does not stop a run cleanly. It took four presses and printed
tracebacks, and hours 13 and 14 of 2026-09-17 were ingested after the first
press although the log had stopped at hour 03, which fits
`ProcessPoolExecutor.shutdown(wait=True)` draining queued work. Follow-up:
catch `KeyboardInterrupt` in `run_pool`, cancel queued futures, exit 130. After
an interrupt the run log can miss hours (stdout is block-buffered into `tee`;
use `python -u`), so the manifest is the record. `pgrep -af backfill.py` does
not match spawn workers; use `pgrep -af 'multiprocessing|backfill'`.

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
- Draft PR #1 open; CI green with pinned ruff 0.17.1 (its default rules, via
  `ruff.toml` and `requirements-dev.txt`), `compileall`, and the offline checks
  now running in CI.
- `download(url, dest, user_agent, timeout, min_bytes, opener)`: tested offline
  with a fake opener (bytes and count, User-Agent sent, short read, tiny file)
  and the real `urlopen` on a `file://` URL. Live on 2026-09-01 hour 0: 6,905,011
  bytes, equal to the server's `Content-Length`; gzip intact.
- `decompress(gz, dest)`: writes the plain file and returns its line count via
  `count_lines`; a truncated gzip raises one of the retried gzip errors. Live on
  the same hour: 51,582, equal to the Parquet rows.
- Manifest: `marker_path`, `write_marker` (local temp JSON, then DuckDB `COPY`
  to the target), `read_marker` and `read_state` (explicit columns,
  `hive_partitioning=false`, `glob()` first so a manifest with no markers
  returns `{}`). Offline checks cover an empty manifest, overwriting a marker,
  mixed nulls across files, an `s3://` write via a fake connection, and no
  stray local directory for an `s3://` path. Live on RadosGW under
  `s3://ghpulse/_scratch/manifest-test`: empty state, overwrite and read-back
  all as expected.
- Suite: **60 PASS, 0 FAIL.**

## Remaining, in order

Done since the last update: `present_partitions`, `ingest_one`, the CLI, the
pilot (4 hours at 1 worker, 8 at 4 workers; about 2.3× by rows) and both weekly
backfills. What is left:

1. **Interrupt-and-resume test.** Start a backfill over a fresh range, stop it
   with Ctrl-C partway, re-run it, and confirm that `--status` ends with every
   hour accounted for and nothing re-downloaded that had a marker.
2. **Progress bars.** Add `SET enable_progress_bar=false` in `_worker`; the
   bars clutter the run log.
3. **Merge PR #1** with "Squash and merge" once those two are done.

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
  DuckDB treats them is unknown. First evidence: 2026-09-01 hour 0 has none, and
  its file ends without a trailing newline (`count_lines` handles that; `wc -l`
  undercounts by one). If `source_lines` and `rows` disagree on real data, look
  here first.
- **`ignore_errors=true` and silent drops.** `source_lines` is recorded beside
  `rows` as a precaution against malformed lines being dropped without notice.
  First evidence: for 2026-09-01 hour 0, `count_lines` gives 51,582, equal to
  the rows written, so nothing was dropped there. One hour is a single reading;
  the full week's markers will show whether it holds.
- **Scratch objects.** Two test markers remain under
  `s3://ghpulse/_scratch/manifest-test/` (96 and 60 bytes). A dry-run delete
  listed exactly those two; remove them with the same `aws s3 rm` without
  `--dryrun`, or leave them.
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
  that the next commit fixed, and a restore commit. Resolved without a rewrite:
  the branch is pushed as-is and will merge with "Squash and merge".
- **Repo copies of the docs** were synced on 2026-10-19 (`PROJECT.md` and
  this page; the repo had matched the original project copy). Keep both in step
  when either changes.

## Step 0 is done when

- Interrupting the backfill and re-running completes the remainder without
  reprocessing what landed.
- `--status` shows ingested, absent, failed and drift hours distinctly.
- The offline checks cover `plan`, `classify`, the counters and the guards, and
  CI is green on the merged branch.
- A pilot has set the worker count from a measurement.
