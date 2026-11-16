# Milestone 1, step 4: data-quality gates (decisions, progress, open items)

Status as of 2026-11-16: **all gates built and verified live.** Branch `quality`,
136 PASS, ruff clean. Next: docs sync, PR and squash merge.

Done (each batch test-first against a stub, except where noted):

- `marker_violations(rec)`: status must be `ingested`, every table's counts
  present, `_in = _bots + _dups + _rows`. Five checks.
- `table_violations(con, table, files)`: nulls in `COMMON_REQUIRED` plus each
  table's `REQUIRED` columns; `count(id) - count(DISTINCT id)` across all files
  given, so cross-hour duplicates count; `[bot]` actors; rows whose `dt`/`hour`
  disagree with their path. Seven checks against the `silver_one` fixture and
  corrupted copies from `tests/quality_fixtures.py` `bad_silver_file`.
- `storage_violations(con, silver_root, rec)`: each table's file exists and
  holds the marker's row count. Three checks.
- `path_exists(con, path)` and `load_marker(con, path)`, added after the first
  live run failed (below). `path_exists` lists the parent folder and looks for
  the exact path; `load_marker` parses the file with `read_text` and
  `json.loads`, so values come back exactly as stored. Five checks.
- CLI `python ingest/quality.py --start … --end …` (`--silver-root`,
  `--manifest-root`): per hour, marker gate and storage gate; then the table
  gate over all built hours at once; prints violations, exits 1 if any. Two
  checks call `main()` and one runs the file as a script.

**Live, 2026-11-16:** Sep 22–28, `checked 168 hours, 168 built: 0 violations`,
exit 0. Negative control Sep 21 23:00 – Sep 22 00:00: `2026-09-21 23: no silver
marker`, exit 1. `read_text` reads `s3://` paths.

**What the first live run found** (none of it visible offline):

- `glob()` on an exact `s3://` path returns the path whether or not the object
  exists (verified: 1 for a missing marker, while `aws s3 ls` showed nothing);
  locally it does check. A wildcard listing of the parent folder is reliable.
- `read_json` on a marker under `hour=00` returns `hour` as the string `'00'`
  unless `hive_partitioning=false` (`hour=12` came back as an integer, which is
  why the fixture passed). It also turns `"2026-09-23"` into a DATE. Hence
  `load_marker` parses the JSON itself.
- An S3 gateway fault ("Server returned nothing") interrupted the first attempt;
  fixed on the cluster side.

**aider incidents in this step** (rules now in `PROJECT.md`): the model wrote
aider's system prompt into the test file (restored, `403dcfd`); an anchor that
was not adjacent in the real file put `storage_violations`, `main` and the
`__main__` guard above `table_violations`, which only a script run exposed; the
guard move failed twice through aider and was done with an assert-guarded
script (`df8d4a2`); three later prompts (R5–R7) had to be resent because the
first attempt never applied. `main` still sits above the gates it calls, which
is harmless and was left alone.

Exit criterion (from `milestone_01.md`): data-quality gates run in CI and still
touch neither the network nor object storage. `PROJECT.md`: CI validates the
code, not the data.

## Design (as agreed)

**Gates** in a new `ingest/quality.py`. Each gate is a function taking a DuckDB
connection (and the relevant roots or relation) and returning a list of
violation strings; an empty list is a pass.

- **Reconciliation:** for every marker and table, `_in = _bots + _dups + _rows`.
- **Required columns never null:** `id`, `created_at`, actor and repo columns,
  `dt`, `hour`, plus per-table keys (`action`, `pr_number`, `issue_number`,
  `review_id`, …). Optional columns (`from_fork`, `head_repo_id`) get their
  null rate reported, not gated.
- **Unique ids across the range checked**, not only within an hour (the
  cross-hour check promised in step 3).
- **No bot leakage:** no silver `actor_login` ends in `[bot]`.
- **Partitions match:** each row's `dt` and `hour` equal its storage path.
- **Storage matches markers:** rows in each Parquet file equal the marker count.

**Two ways to run them:**

1. **CI, offline:** fixtures built in a temp directory, reusing the `silver_one`
   fixture in `tests/test_transform.py`. The good fixture passes every gate;
   each bad fixture (null id, id duplicated across two hours, leaked bot row,
   mislabelled partition, marker count off by one) is caught by the right gate.
2. **Live, by hand:** `python ingest/quality.py --start … --end …` runs the same
   gates over real silver on Ceph and exits non-zero on any violation. A
   scheduled job with a service account is the later home for this, not PR
   checks.

Left out unless asked for: null-rate thresholds on optional columns, and volume
checks (overlap with the `stable` flag).

## Working notes

- Same build pattern as steps 0 and 3: test-first against a stub returning
  wrong values, one aider prompt per change, open only the file being edited,
  `--map-tokens 0` for insertions. File sizes at the start: `tests/test_transform.py`
  443 lines, `ingest/silver.py` 153.
- Housekeeping still open: the squash merge of step 3 used GitHub's default
  message (commit list), so set the repo's squash default to "Pull request
  title and description".
