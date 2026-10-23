# Milestone 1, steps 1–2 — backfill and measurements

Status as of 2026-10-23. Two weeks of bronze are ingested; the measurements in
`milestone_01.md` step 2 have been taken on the week that passed a stability
check. Single week, so every number here is one reading.

## What was ingested

| Week | Hours | Status | Rows | `rows = source_lines` |
|---|---|---|---|---|
| 2026-09-01 … 09-07 | 168 | 168 ingested, 0 absent, 0 failed | 8,838,510 | all 168 |
| 2026-09-22 … 09-28 | 168 | 168 ingested, 0 absent, 0 failed, 0 retried | 14,908,369 | all 168 |

Every download matched the server's `Content-Length`. So for these 336 hours,
DuckDB's `ignore_errors=true` dropped nothing and there were no blank lines:
the open items about silent drops are answered for this data.

Throughput, single readings: the pilot gave about 2.3× by rows going from 1 to 4
workers; the Sep 1–7 run took 10 min 45 s for 156 hours at 4 workers.

## Why the measurement week moved to Sep 22–28

Sep 1–7 is a faithful copy of what GH Archive published, but the archive went
through distinct capture regimes that week:

| Period (UTC) | Events/h | Push share | Non-push events/h |
|---|---|---|---|
| Sep 1, 00–11 | 31k–71k | ~95% | ~2k |
| Sep 1 12:00 → Sep 3 ~20:00 | 1.5k–2.5k | 0 | ~2k |
| Sep 3 21:00 → Sep 5 ~06:00 | 54k–78k | 0 | 54k–78k |
| Sep 5 06:00 → Sep 7 | 73k–115k | rising from 1% to 94% | falling from ~70k to ~7k |

PushEvent, CreateEvent and DeleteEvent were absent entirely from Sep 1 12:00
until Sep 5 morning (95 of 168 hours have zero pushes). The cause is unknown;
no public record of it was found. Daily 12:00 file sizes from 1 Sep to 15 Oct
show further irregular stretches (Sep 4–6 and Sep 11–13 at 45–85 MB, dips on
Sep 21, Oct 7–10 and Oct 15) against 12–25 MB on most days; Sep 22 – Oct 6 was
the steadiest run.

Funnel stage 0 is defined by PushEvent, and non-push events are the
recommender's signal, so measuring Sep 1–7 would mostly measure the archive's
failure modes. Decision (agreed 2026-10-23): measure on Sep 22–28; keep Sep 1–7
ingested as the documented example of upstream regime shifts.

Correction recorded: `milestone_01.md`'s "PushEvent is 95% of bronze" is
consistent with the steadier hours seen (94–97% on Sep 1 00–11, Sep 7 evening
and Sep 14 12:00). An earlier reading of 36.5% came from averaging across Sep
1–7's broken regimes and was wrong to treat as a contradiction. Sep 22–28
itself runs at 64–87% pushes per hour, so the mix also varies between ordinary
periods.

## Measurement 1 — duplicate event ids (Sep 22–28)

| Scope | Duplicate rows | Rate |
|---|---|---|
| within an hour | 5 | 0.00003% |
| within a day | 5 | 0.00003% |
| across the week | 5 | 0.00003% |

All five duplicates sit inside single hourly files; none crosses a file
boundary. This supports deduplicating within each hour. A cross-hour duplicate
count should still run as a data-quality gate, since one week cannot rule out
reissued files.

## Measurement 2 — bot share, `[bot]` suffix heuristic (Sep 22–28)

- **18.02% of events** come from actors whose login ends in `[bot]`.
- **8,377 of 1,799,662 distinct actors (0.47%)** carry the suffix.
- Per day the event share stays between 17.2% and 19.5%, so bots do not explain
  the weekend volume rise (Sat 26 midday to Mon 28 early, about 1.5×, at a
  steady push share), which remains unexplained.

The suffix only catches self-declared bots, so these are lower bounds. The
single hour used in `milestone_01.md` (4.8% of events, 0.25% of actors)
understated bot activity considerably.

## Measurement 3 — Parquet size (Sep 22–28)

- 2.45 GB for the week; 289–394 MB per day.
- Per hour: 8.3 MB minimum, 20.7 MB maximum, 14.6 MB mean.
- Projected month (mean × 720): **about 10.5 GB of ZSTD Parquet**, in line with
  the ~10 GB gzipped source estimate.

## Measurement 4 — hourly completeness

Quantiles over hours (min, 5%, median, 95%, max):

| Week | Zero-push hours | Push share | Non-push events per hour |
|---|---|---|---|
| Sep 1–7 | 95 of 168 | 0, 0, 0, 0.942, 0.974 | 1,193 / 1,469 / 18,567 / 79,670 / 82,045 |
| Sep 22–28 | 0 of 168 | 0.638, 0.718, 0.808, 0.855, 0.871 | 8,398 / 10,949 / 16,893 / 23,764 / 27,861 |

## Proposed stability rule (not yet adopted)

An hour is usable for behavioural analysis when:

1. it has at least one PushEvent;
2. its push share lies between 0.50 and 0.95;
3. its non-push event count lies within a factor of 3 of the reference median
   (16,893 from Sep 22–28, so between about 5,600 and 50,700).

Every Sep 22–28 hour passes. Before adopting it, count how many Sep 1–7 hours
pass: the expectation is very few, in the recovered stretch of Sep 6–7. Hours
such as Sep 14 12:00 (94% pushes, ~5.7k non-push) sit near the edges, so the
bounds need checking against more periods before choosing the month. Bronze is
never filtered; the rule applies from silver onwards and is recorded with the
data.

## Open questions

- What causes the capture regimes, and do they recur in the candidate month?
- Why does weekend volume rise in Sep 22–28?
- How many unsuffixed bots are there? The heuristics in `milestone_01.md`
  (event rate, naming patterns) are still to do.
