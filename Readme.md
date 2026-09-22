# ghpulse

A data pipeline over [GH Archive](https://www.gharchive.org/) — every public
GitHub event, hourly — running on self-hosted infrastructure.

This is milestone 0: **one hour of data, end to end, queryable.** It exists to
prove the path works before any of it is worth scaling.

## The question

Where do open-source projects leak contributors, and can we recommend
projects a contributor would actually engage with?

Milestone 0 answers something much smaller: *what happened on GitHub in one
hour, and can we query it from a laptop?*

## Architecture

```
gharchive.org/YYYY-MM-DD-H.json.gz
        │  fetch.py — download, verify, convert
        ▼
   BRONZE   s3://ghpulse/bronze/events/dt=…/hour=…/*.parquet
        │  immutable, one row per event, schema-on-read
        ▼
   DuckDB   analysis/queries.sql — read directly from object storage
```

Bronze only, for now. Silver and gold arrive when there is enough data for
the shape of the questions to matter.

## Design decisions

**Parquet, not JSON.** The source is gzipped JSON, ~100MB/hour. Parquet with
ZSTD lands at roughly a fifth of that and, more importantly, is columnar — a
query touching three fields reads three columns, not every byte. That is the
difference between a query taking seconds and taking minutes once this is a
year of data.

**Object storage, not a database.** Ceph RadosGW via S3. Compute and storage
stay decoupled: Spark, DuckDB, and a Kubeflow notebook read the same bytes
with no serving layer between them. A database would mean one engine,
one machine, and an import step.

**DuckDB for the conversion, not Spark.** 100MB does not need a distributed
engine. Spark's startup alone costs more than this whole job. Spark earns
its place at the silver and gold layers, across months of data — using it
here would be cargo-culting.

**Partitioned by `dt` and `hour`.** Queries are nearly always time-bounded,
and partition pruning turns "scan everything" into "open one file". The
partition scheme is the single most consequential decision in the layout.

**Bronze is immutable.** Re-running a given hour overwrites that partition
and nothing else. If silver logic turns out to be wrong — and it will —
reprocessing costs nothing because the raw data is still there. This is why
bronze exists at all rather than transforming on ingest.

## What went wrong building this

Three bugs, each caught differently, each costing one re-run of one
partition. That is bronze immutability earning its keep — the schema
decision was wrong twice and neither time required a migration.

**GH Archive 403s urllib's default user-agent.** The same URL returned 200
to `curl` and 403 to Python, so a working file looked like a missing one.
Fixed by setting a `User-Agent` header.

**`read_json_auto` merges every event type's payload into one struct.**
PushEvent's `commits` beside IssuesEvent's `issue` beside
PullRequestEvent's `pull_request` — roughly 200 fields, nearly all NULL per
row. Querying it crashed DuckDB outright. Bronze now stores `payload` as
JSON text: preserve everything, commit to nothing, and let silver extract
typed columns per event type.

**A "harmless" optimisation broke string matching silently.** Adding
`maximum_depth=2` to limit inference cost also stopped `actor` being parsed
as a struct, so `actor.login` returned a quoted JSON value — `'"dave[bot]"'`
rather than `dave[bot]`. Every string comparison stopped matching. Seven of
eight tests still passed, because `count(DISTINCT actor.login)` returns 4
either way. Only the bot filter caught it. Against real data this would have
produced a funnel with zero bots filtered and no reason to suspect anything.

That last one is why the test suite checks values and not just shapes.

## Known limitations

**`init.sql` materialises bronze into memory rather than querying it in
place.** DuckDB 1.5.5 crashes on any `GROUP BY` with more than one
`count(DISTINCT)` when reading remote Parquet — an upstream bug, narrowed
by elimination and documented in `analysis/init.sql`. Workable at hour and
day scale, not at month scale, which is one of several reasons aggregation
moves to Spark in milestone 1.

**The bot filter only catches the `[bot]` suffix.** Plenty of automation
does not follow that convention, so human counts are an upper bound.

**One hour is not enough data to answer the actual question.** The funnel
query works but says little: nobody becomes a maintainer in an hour, and
`PushEvent` is ~95% of all activity.

## Layout

```
ghpulse/
├── README.md              this file
├── docs/RUNBOOK.md        how to run it, and what breaks
├── ingest/fetch.py        gharchive -> bronze parquet
└── analysis/queries.sql   the questions, in SQL
```

## Running it

See `docs/RUNBOOK.md`. Briefly:

```bash
s3env                                   # load S3 credentials
python3 ingest/fetch.py 2026-09-14 12   # one hour -> bronze
duckdb -init analysis/init.sql          # query it
```
