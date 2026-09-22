# Milestone 1 — a month of data, and a silver layer

Milestone 0 proved the path. This one makes the question answerable.

---

## Why a month, and why now

The recommender needs an interaction matrix: actor × repo, weighted by
event type. One hour gives 17,948 actors and 22,661 repos with almost no
overlap — a matrix that is essentially all zeros and cannot be factorised
into anything meaningful.

A month gives repeat interactions, which is the entire signal. It is also
the minimum for the funnel question, since "repeat contributor" is defined
across months and "first PR" needs history to be *first* relative to.

Roughly: 720 hourly files, ~70GB gzipped source, ~12GB Parquet.

---

## What changes

**Spark replaces DuckDB for transformation.** Not because Spark is better,
but because 12GB across 720 files exceeds what fits in memory on one box,
and the silver transforms involve shuffles — dedup across partitions,
windowing per actor — which is exactly what Spark is for. DuckDB stays for
interactive analysis over the much smaller gold layer.

This is also forced by the DuckDB remote-Parquet bug documented in the
README: materialising bronze stops being viable well before a month.

**A silver layer appears.** Bronze is raw and ragged by design. Silver is
typed, deduplicated, bot-filtered, one table per event type:

```
silver/pull_requests/dt=…     actor, repo, action, additions, deletions, merged
silver/issues/dt=…            actor, repo, action, number, title
silver/stars/dt=…             actor, repo
silver/reviews/dt=…           actor, repo, pr_number, state
```

Only the event types the question needs. `PushEvent` is 95% of bronze and
contributes almost nothing to a contribution funnel — carrying it into
silver would multiply cost for no analytical gain.

**Gold becomes the interaction matrix and the funnel facts.**

```
gold/interactions/            actor, repo, weight, first_seen, last_seen
gold/contributor_funnel/      actor, repo, month, stage
```

---

## Things that will need deciding

**Deduplication window.** GH Archive occasionally reissues files. Dedup on
event `id`, but across what range — within an hour, a day, the whole month?
Cheaper is narrower; correct is wider. Needs measuring rather than guessing:
count duplicate ids across a week first.

**Interaction weights.** watch < issue < PR opened < PR merged is the
obvious ordering, but the actual numbers are a modelling choice that
affects every downstream result. Worth being explicit and version-controlled
rather than buried in a transform.

**Bot identification.** The `[bot]` suffix catches some. Others need
heuristics — event rate, account age, naming patterns. Getting this wrong
skews the recommender badly, since bots interact with enormous numbers of
repos and will dominate any co-occurrence signal.

**Backfill mechanics.** 720 downloads is long enough to need resumability,
parallelism, and a record of which hours succeeded. The current `fetch.py`
handles one hour at a time with no state.

---

## Order of work

1. **Backfill a week first, not a month.** Enough to measure duplicate
   rates, bot share, and actual Parquet sizes. Cheap to redo if the silver
   schema is wrong — and on current form it will be at least once.
2. Silver transforms in Spark, submitted via `spark-client`.
3. Data quality gates: row counts, null rates, PK uniqueness, in CI.
4. Extend to a month once the shape is proven.
5. Gold: interactions matrix and funnel facts.
6. Baseline recommender — most-popular, then item-kNN. Do not skip these.

---

## What success looks like

A query that answers, across a month: how many contributors opened their
first PR, how many had one merged, and how many came back. Plus an
interaction matrix dense enough that ALS produces something that beats
most-popular on Recall@10.

If it does not beat most-popular, that is a finding worth reporting rather
than hiding.
