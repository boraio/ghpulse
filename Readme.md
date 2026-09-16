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
