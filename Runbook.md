# Runbook — milestone 0

One hour of GH Archive, landed as Parquet on Ceph, queried with DuckDB.
Roughly twenty minutes, most of it download time.

---

## Prerequisites

```bash
duckdb --version                    # 1.5.x, from install.duckdb.org
python3 -c "import duckdb; print(duckdb.__version__)"
s3env                               # exports AWS_* for RadosGW
aws s3 ls                           # must succeed before anything else
```

If `aws s3 ls` fails, nothing downstream will work. Check
`sudo grep ^S3_ /etc/jenga.env` holds real values rather than placeholders,
and that RadosGW is up: `curl -s -o /dev/null -w '%{http_code}\n'
http://10.0.20.252:80` should return 200 or 403.

If the Python module is missing: `pip install duckdb`.

---

## 1. Create the bucket

Once, and separately from ingestion — creating buckets is an administrative
act, not something a data pipeline should do on the fly.

```bash
s3env
aws s3 mb s3://ghpulse
aws s3 ls
```

## 2. Land one hour

```bash
python3 ingest/fetch.py 2026-09-14 12
```

Expect roughly:

```
ghpulse: 2026-09-14 hours 12..12 -> s3://ghpulse/bronze/
  https://data.gharchive.org/2026-09-14-12.json.gz
    187,432 events  (98.4 MB gz)  -> s3://ghpulse/bronze/events/dt=2026-09-14/hour=12/data.parquet

total: 187,432 events
```

Verify independently of the tool that wrote it:

```bash
aws s3 ls s3://ghpulse/bronze/events/dt=2026-09-14/hour=12/
```

Parquet + ZSTD should be well under a fifth of the gzipped JSON.

## 3. Query it

```bash
duckdb -init analysis/init.sql
```

```sql
SELECT count(*) FROM events;
.read analysis/queries.sql
```

## 4. Prove it is idempotent

Re-run the same hour and confirm the count is unchanged, not doubled:

```bash
python3 ingest/fetch.py 2026-09-14 12
duckdb -init analysis/init.sql -c "SELECT dt, hour, count(*) FROM events GROUP BY 1,2"
```

This is the property that makes backfills safe to retry, and it is worth
checking rather than assuming.

## 5. Extend to a day

```bash
python3 ingest/fetch.py 2026-09-14 0 23
```

~2.3GB of source, landing around 400MB of Parquet, a few million events.

---

## What breaks

**`AWS_ACCESS_KEY_ID not set`** — `s3env` was not run, or ran in a different
shell. It exports into the current shell only.

**`HTTP Error 404`** — that hour does not exist upstream. GH Archive has
genuine gaps; the script skips and continues rather than aborting a
backfill.

**Credential errors that mention nothing about URLs** — usually
`s3_url_style` is not `path`. RadosGW does not support virtual-host
addressing, and the error does not say so.

**Row counts far lower than expected** — a gzip reader stopping at the first
member. This is why `fetch.py` decompresses to disk instead of reading the
`.gz` directly.

**Schema errors on some hours** — an event type with a field the first rows
lacked. `union_by_name=true` handles it; without that flag the failure looks
like corrupt data.

---

## Next

Milestone 1 is a month of data and a silver layer: typed, deduplicated,
bot-filtered tables per event type. That is the point at which Spark starts
to earn its keep and the funnel question becomes answerable.
