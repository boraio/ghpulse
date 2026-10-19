# ghpulse

A contributor-funnel analysis and recommender built on GH Archive, running on a
self-hosted production-mimicking data platform.

## The question

Most people who touch a GitHub repository never contribute to it. They push to
their own fork, star it, open one issue and leave. A small fraction become
reviewers and maintainers.

**Where do people drop out of the contribution funnel, and can we predict which
repositories a given actor is likely to engage with next?**

The funnel, as observed in GH Archive event types:

| Stage | Signal |
|---|---|
| 0 pusher only | PushEvent, nothing else |
| 1 observer | WatchEvent, ForkEvent |
| 2 reporter | IssuesEvent, IssueCommentEvent |
| 3 contributor | PullRequestEvent |
| 4 reviewer | PullRequestReviewEvent, PullRequestReviewCommentEvent |

From one hour of data (2026-09-14 12:00, 100,774 events) the funnel is brutally
top-heavy: 16,943 pusher-only actors, 106 observers, 169 reporters, 380
contributors, 95 reviewers. That shape is the thing worth modelling.

The recommender is the point of the project, not a bolt-on. Implicit feedback,
no explicit ratings, severe popularity skew — a realistic setting rather than a
MovieLens exercise.

## Why this architecture

**Medallion: bronze → silver → gold.** Bronze is immutable raw events per
partition. Silver is typed, deduplicated, bot-filtered extraction per event
type. Gold is the analysis-ready interactions matrix and funnel facts.

**DuckDB for bronze, Spark for silver onward.** 100 MB per hour does not
justify a distributed engine, and saying so is part of the point — reaching for
Spark at that scale would be the wrong engineering call. Spark earns its place
at milestone 1, where the silver transforms need shuffles (dedup across
partition boundaries, windowing per actor over a month) rather than scans.

**Bronze is immutable per-partition.** Reprocessing after a schema change costs
one re-run of the affected partitions, not a migration. This has already paid
for itself twice.

**Object storage is Ceph RadosGW (S3-compatible)**, not a cloud bucket. The
platform is self-hosted; that constraint is deliberate and part of what the
project demonstrates.

## Where it runs

- **Bronze ingest**: `ingest/fetch.py`, DuckDB + httpfs, writes Parquet to
  `s3://ghpulse/bronze/events/dt=<date>/hour=<HH>/data.parquet`
- **Object storage**: Ceph RadosGW at `http://10.0.20.252:80`, bucket `ghpulse`
  (`sila` is an older Spark artifact bucket, not used here)
- **Credentials**: `AWS_*` env vars loaded by the `s3env` shell helper from
  `/etc/jenga.env`. Never in the repo.
- **Spark**: `spark-client` against the Kubernetes cluster (not Kyuubi — see
  non-goals)
- **CI**: GitHub Actions on self-hosted runners in the cluster (ARC)
- **Repo**: `boraio/ghpulse`

## Current state

**Milestone 0 is complete and verified.** One hour end to end: download,
transform, land on Ceph, query from DuckDB. 100,774 events, 17,948 actors,
22,661 repos, 16 event types. Nine offline checks green. CI passing.

Everything after this is ahead of us.

## Milestones

### Milestone 0 — bronze, one hour ✅

One hour of GH Archive as Parquet on Ceph, partitioned by date and hour,
queryable from DuckDB, with offline tests in CI.

### Milestone 1 — a month of data, and a silver layer

See **`milestone_01.md`** for scope, the four open decisions, order of work and
exit criteria. In short: a resumable backfill with a manifest, a week ingested
and measured before committing to a month, silver tables for the four
funnel-relevant event types, and a `most-popular` baseline evaluated on a
temporal split.

### Milestone 2 — the recommender

- Item–item kNN on co-occurrence
- Spark ALS on implicit feedback
- Optionally a two-tower model if the earlier stages justify it

**Exit criteria**: a table comparing most-popular, kNN and ALS on the same
temporal split with the same metrics. A model that beats the baseline on
Recall@10 *without* collapsing catalogue coverage. If it doesn't beat the
baseline, that result is reported honestly rather than buried.

### Milestone 3 — serving and the write-up

- Model artifact and recommendations landed somewhere queryable
- A README a reviewer can read in five minutes and come away knowing the
  question, the pipeline, the evaluation protocol and the result
- Optionally a small dashboard over the funnel

## Evaluation discipline

These are decided up front because they are what a reviewer will question
first, and because deciding them after seeing results is how people fool
themselves.

- **Temporal split, never random.** Train on earlier weeks, evaluate on the
  last. A random split lets the model see the future and inflates every metric.
- **A stated baseline, committed before any model is fitted.** `most-popular`
  on the same split. Nothing counts as an improvement until it beats this.
- **Popularity bias measured, not assumed away.** Report catalogue coverage and
  the mean popularity rank of recommended items alongside accuracy. A model
  that recommends only the top 100 repos can score well and be useless.
- **Bots excluded before training.** They interact with enormous numbers of
  repositories and will dominate any co-occurrence signal.

## Hard-won facts

Each of these cost real time. Do not relitigate them without evidence.

- **`payload` is cast to JSON text, not left as a struct.** `read_json_auto`
  merges every event type's payload into one ~200-field struct, nearly all NULL
  per row, and DuckDB then crashes with `INTERNAL Error: Unsupported type for
  NumericValueUnionToValue` when querying it.
- **`union_by_name=true` is required.** Payload shapes differ per event type and
  drift as GitHub adds fields. Without it a file whose first rows lack a column
  fails on a later row that has it.
- **Never add `maximum_depth`.** It stops `actor` and `repo` being parsed as
  structs, so `actor.login` returns a quoted JSON value (`'"dave[bot]"'`) and
  every string comparison silently stops matching. This regression passed 7 of
  8 tests when it was introduced.
- **DuckDB reads `.gz` natively.** Verified: `gzip.open` and
  `read_json_auto('…json.gz')` both count 100,774 for 2026-09-14 hour 12. The
  decompress-to-disk step in `fetch.py` is a debugging convenience costing ~10x
  disk, not a correctness requirement. There is no multi-member gzip hazard —
  that claim was fabricated by a local model and tested false.
- **GH Archive URLs zero-pad the month but not the hour** —
  `2026-09-01-5.json.gz`, not `-05`. Getting it wrong yields a 404 that looks
  like missing data.
- **GH Archive 403s `urllib` without a `User-Agent` header**, while returning
  200 to curl for the same URL.
- **Grouped multi-`count(DISTINCT)` over remote Parquet crashes DuckDB.**
  Worked around by materialising bronze into a local table — which is part of
  why a month needs Spark.
- **`tests/test_transform.py` is a standalone script**, run as
  `python tests/test_transform.py`, not pytest. It uses a `check(label, got,
  want)` helper and four deliberately ragged synthetic events. Add checks
  inside `main()`; never restructure it.
- **CI validates the code, not the data.** Nothing in `tests/` touches the
  network or object storage. Anything needing S3 credentials belongs in a
  scheduled job with a service account, not a PR check.
- **DuckDB's init file must not hard-fail.** `~/.duckdbrc` loads `httpfs` only;
  the S3 secret lives in `~/.duckdb-s3.sql` and is read on demand, so plain
  `duckdb` works with no credentials loaded.

## Conventions

- Virtualenv is `.ghpulsevenv`. Activate it before running anything — including
  before launching any assistant that will invoke `pytest` or `ruff`, or it
  resolves the system binaries and reports misleading failures.
- Dependencies in `requirements.txt`; dev tools in `requirements-dev.txt`.
- Comments explain **why**, not what the next line does. Never state a claim
  about a tool's behaviour that hasn't been verified.
- Secrets live in `/etc/jenga.env`, loaded via shell helpers. `.gitignore`
  excludes `.env`, `*.pem`, `*.key`, `credentials`, `.ghpulsevenv/`.

## Non-goals

- **Real-time.** Batch-first, deliberately. Streaming would be a different
  project.
- **Kyuubi.** Abandoned after an unresolved `/opt/pebble/pebble` engine-launcher
  bug in the charm. `spark-client` works and is what we use.
- **Beating state of the art.** The goal is a defensible pipeline with an honest
  evaluation, not a leaderboard position.
- **Deep learning for its own sake.** A two-tower model only if the simpler
  stages justify the step up.
