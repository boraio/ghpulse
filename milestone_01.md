# Milestone 1 — a month of data, and a silver layer

Milestone 0 proved the path. This one makes the question answerable.

---

## Why a month, and why now

The recommender needs an interaction matrix: actor × repo, weighted by event
type. One hour gives 17,948 actors and 22,661 repos with almost no overlap — a
matrix that is essentially all zeros and cannot be factorised into anything
meaningful.

A month gives repeat interactions, which is the entire signal. It is also the
minimum for the funnel question, since "repeat contributor" is defined across
months and "first PR" needs history to be *first* relative to.

**Sizing, from measurement rather than estimate.** One hour (2026-09-14 12:00)
is 14.3 MB gzipped and 100,774 events. A month of 720 hours is therefore around
**10 GB gzipped**, roughly 100–140 GB expanded. Parquet output per hour has not
been measured yet — that is one of the three measurements in step 1 below, and
until it exists any month-level Parquet figure is a guess.

---

## What changes

**Spark replaces DuckDB for transformation.** The reason is shuffles, not
volume. Silver needs deduplication across partition boundaries and windowing
per actor over the whole month — operations that fan data across the dataset
rather than scanning it. That is what Spark is for. A bare 10 GB of Parquet
would in fact fit in memory on one box, so "too big for DuckDB" is not an
honest justification and should not be claimed; "the wrong shape for a
single-node engine" is.

DuckDB stays for interactive analysis over the much smaller gold layer, which
is where it is genuinely the better tool.

There is also a practical forcing function: the DuckDB remote-Parquet crash
documented in the README means bronze has to be materialised locally to be
queried, and that stops being viable well before a month.

**A silver layer appears.** Bronze is raw and ragged by design. Silver is
typed, deduplicated, bot-filtered, one table per event type:

```
silver/pull_requests/dt=…     actor, repo, action, additions, deletions, merged
silver/issues/dt=…            actor, repo, action, number, title
silver/stars/dt=…             actor, repo
silver/reviews/dt=…           actor, repo, pr_number, state
```

Only the event types the question needs. `PushEvent` is 95% of bronze and
contributes almost nothing to a contribution funnel — carrying it into silver
would multiply cost for no analytical gain. It stays in bronze, where it costs
nothing extra and remains available if that judgement turns out to be wrong.

**Gold becomes the interaction matrix and the funnel facts.**

```
gold/interactions/            actor, repo, weight, first_seen, last_seen
gold/contributor_funnel/      actor, repo, month, stage
```

---

## Things that will need deciding

Each of these is a modelling choice that affects every downstream result. All
four should be resolved by measurement, written down, and version-controlled —
not buried in a transform.

**Deduplication window.** GH Archive occasionally reissues files. Dedup on
event `id`, but across what range — within an hour, a day, the whole month?
Cheaper is narrower; correct is wider. Measure duplicate `id` rates at all
three scopes across a week before choosing.

**Interaction weights.** `watch < issue < PR opened < PR merged` is the obvious
ordering, but the actual numbers are an assumption, not a fact. Put them in one
place as named constants so a reviewer can see them and so sensitivity can be
tested later.

**Bot identification.** The `[bot]` suffix catches some. One hour of data shows
255 bot actors producing 4,794 events — 0.25% of actors generating 4.8% of
events. Others need heuristics: event rate, account age, naming patterns.
Getting this wrong skews the recommender badly, since bots interact with
enormous numbers of repos and will dominate any co-occurrence signal.

**Backfill mechanics.** 168 downloads for a week, 720 for a month, is long
enough to need resumability, bounded parallelism, and a durable record of which
hours succeeded, which were absent upstream, and which failed. The current
`fetch.py` processes a contiguous hour range in one process with no state: a
failure halfway means starting over, and nothing afterwards can tell which
partitions exist.

---

## Order of work

**Step 0 — backfill mechanics, before any backfill.** You cannot ingest a week
with a script that holds no state. This is a prerequisite, not step 4. It needs
a decision on where the manifest lives (a file in the bucket versus deriving
state from object listing — listing is self-healing but slow, a manifest is
fast but can drift), resume-versus-force semantics, a concurrency limit that
does not get us throttled, and a distinction between "GH Archive never
published this hour" (normal, record it) and "the transfer failed" (retry).

1. **Backfill a week, not a month.** `2026-09-01` to `2026-09-07`. Enough to
   measure duplicate rates, bot share, and actual Parquet sizes. Cheap to redo
   if the silver schema is wrong — and on current form it will be at least once.
2. **Measure.** Duplicate `id` rate within an hour, within a day, across the
   week. Bot share of events and of distinct actors. Parquet bytes per hour and
   per day. These three numbers settle the dedup window, the bot filter, and
   the month-level sizing respectively.
3. **Silver transforms in Spark**, submitted via `spark-client`.
4. **Data quality gates in CI**: row counts, null rates, primary-key
   uniqueness. Still offline — synthetic fixtures, no object storage.
5. **Extend to a month** once the shape is proven.
6. **Gold**: interactions matrix and funnel facts.
7. **Baseline recommender** — most-popular, then item-kNN. Do not skip these.
   The baseline is what makes every later number mean something.

---

## Exit criteria

Milestone 1 is done when all of these hold:

- A month of bronze is present, with a manifest that distinguishes ingested,
  absent-upstream and failed partitions. Gaps are visible rather than silent.
- The backfill is resumable: interrupting it and re-running completes the
  remainder without reprocessing what landed.
- Silver tables exist for the four event types above, bot-filtered and
  deduplicated on a window chosen from measured duplicate rates.
- Bronze → silver row counts reconcile, with any discrepancy explained by the
  dedup and bot filters rather than unaccounted for.
- Data quality gates run in CI and still touch neither the network nor object
  storage.
- The gold interactions table exists, and `most-popular` has been evaluated on
  a temporal split with Recall@10 and NDCG@10 written down in the repo.

That last point is the real gate. Without a baseline number committed before
any model is fitted, nothing afterwards can be shown to be an improvement.

---

## What success looks like

A query that answers, across a month: how many contributors opened their first
PR, how many had one merged, and how many came back. Plus an interaction matrix
dense enough that ALS produces something that beats most-popular on Recall@10.

If it does not beat most-popular, that is a finding worth reporting rather than
hiding. A negative result with an honest evaluation protocol is worth more than
a positive one with a leaky split.
