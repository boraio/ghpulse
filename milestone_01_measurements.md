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

## Stability check across Sep 1–28 (2026-10-23)

Bronze now covers 2026-09-01…28 (672 hours, 51,290,044 events; `rows =
source_lines` on all 672 markers). The proposed rule was applied per hour.
**417 of 672 hours pass.**

| Days | Hours passing | Pattern |
|---|---|---|
| Sep 1–6 | 0 of 144 | no pushes for most of Sep 1–5 (95 hours); non-push volume either ~2k/hour (Sep 1–3) or 60–80k/hour (Sep 4–6) |
| Sep 7–10 | 62 of 96 | recovers, with dips: Sep 8 13:00–23:00, Sep 9 14:00 to Sep 10 04:00 (11 zero-push hours) |
| Sep 11–12 | 6 of 48 | pushes present but push share 0.14–0.41, non-push 30–80k/hour |
| Sep 13–16 | 71 of 96 | mostly normal, with short low-share runs (Sep 15 13:00 to Sep 16 01:00) |
| Sep 17–20 | 96 of 96 | clean |
| Sep 21 | 14 of 24 | 13:00–22:00 fails, 6 zero-push hours, as low as 2,348 non-push events |
| Sep 22–28 | 168 of 168 | clean (the reference week, so this passes by construction) |

Readings, not conclusions:

- The rule separates the visibly broken stretches from the clean ones, and
  weekends (Sep 19–20, 26–27) pass, so it does not simply reject quiet hours.
- The thresholds come from Sep 22–28, which is circular for that week. Sep
  19–20 sit at push shares of 0.85–0.90, closer to the 0.95 bound than any
  Sep 22–28 day (max 0.871); whether that is normal weekend behaviour or a mild
  regime is unknown.
- **No clean calendar month exists in September.** The longest clean stretch
  is Sep 17 00:00 to Sep 28 23:00, broken only by Sep 21 13:00–22:00. The
  regimes' cause is still unknown.

Decision needed before step 5: what "a month" means in the exit criteria.
Options: (a) 30 contiguous days that pass the rule, found by ingesting forward
into October (data to about 2026-10-22 is available); (b) September with
failing hours masked, which leaves volume dips inside the temporal split; (c)
a shorter clean window. Recommendation: (a) — ingest 2026-09-29…10-21, run
the same check, and pick the window from that.

## Forward ingest and window search, Sep 29 – Oct 22 (2026-10-23)

Ingested 2026-09-29T00…10-22T23: 576 hours, 0 absent, 0 failed, 17 min 53 s at
4 workers. Bronze now covers 2026-09-01…10-22, 1,248 hours. 854 of 1,248 hours
pass the rule.

- **Regime breaks recur.** New failing runs: Oct 6 19:00 to Oct 8 02:00 (32 h),
  Oct 9 15:00 to Oct 10 00:00 (10 h), Oct 14 19:00 to Oct 18 11:00 (89 h, 86 of
  them with no pushes), Oct 22 16:00–23:00 (8 h; possibly too recent, unknown).
  With Sep 1–6, Sep 8–12 and Sep 21, that is a break roughly every one to two
  weeks. Cause unknown.
- **No 30-day window passes in full.** Best day-aligned windows: Sep 14 – Oct
  13 and Sep 15 – Oct 14, 73 failing hours each (10.1%); Sep 7 – Oct 6, 116
  failing hours, almost all in its first ten days.
- **Longest clean run:** Sep 21 23:00 to Oct 6 18:00, 356 hours (14.8 days).

Consequence: "30 contiguous days that pass" is not attainable in this data, so
the month will contain masked hours whichever window is chosen. Proposal, not
yet decided:

1. Silver carries a per-hour `stable` flag computed from this rule; bronze and
   silver are never filtered by it. Gold and evaluation apply it.
2. Choose the window at step 5 under one hard constraint: **the evaluation
   (last) week must be clean.** Sep 14 – Oct 13 fails that (its last week holds
   Oct 7–10); Sep 7 – Oct 6 meets it (test week Sep 30 – Oct 6, 5 failing
   hours), with its masked hours in training.
3. Report the masked share alongside every metric.

Bronze ingest stops here; the next work is silver (step 3), developed on the
clean Sep 22–28 week.

## Payload slimming and capture rate (2026-10-23)

**PR payloads are slim, upstream.** In bronze and in the raw GH Archive file
for 2026-09-23 hour 12 alike, `payload.pull_request` holds only `id`, `number`,
`url`, `head` and `base` (head/base carry `ref`, `sha` and `repo` id/name). No
`merged`, `additions`, `deletions`, `title` or author. This matches GitHub's
Events API change effective 2025-10-07 (GitHub changelog, 2025-08-08); the
changelog does not list the removed fields. A `merged` action exists (77,272
in Sep 22–28). Issues keep the full object (`title`, `user`, `state`); reviews
keep `state` and `user`. Bronze is faithful; nothing is lost in our transform.
Top-level payload keys appear on every row (the merged schema fills nulls), so
only non-null counts mean anything.

**GH Archive holds about 1–2% of PR activity.** Sep 22–28, six repos with the
most non-bot merges in bronze, against GitHub's search API for the same UTC
window:

| Repo | Opened: bronze / GitHub | Merged: bronze / GitHub |
|---|---|---|
| openclaw/openclaw | 74 / 4,129 (1.8%) | 60 / 3,250 (1.8%) |
| merge-demo/mergequeue-st | 164 / 9,260 (1.8%) | 60 / 2,647 (2.3%) |
| microsoft/winget-pkgs | 68 / 4,200 (1.6%) | 55 / 3,673 (1.5%) |
| merge-demo/mergequeue-bazel | 134 / 12,186 (1.1%) | 55 / 4,563 (1.2%) |
| jeong-sik/masc | 22 / 1,575 (1.4%) | 29 / 1,466 (2.0%) |
| CBirkbeck/tauceti-explorer | 16 / 1,387 (1.2%) | 28 / 1,361 (2.1%) |

Consistent with that: only 1,429 of 77,272 merges (1.8%) have their `opened`
event in the same week, 99% of those by the same actor, with a median
open-to-merge gap of 259 s (p75 2,033 s). Capture looks clumped in time:
events seconds apart are kept or lost together, so independent events by one
person days apart rarely both survive. Cause unknown; one search found no
documentation of it.

Caveats: six high-volume repos chosen by bronze activity, PR events only, one
week. Whether the share is uniform across repos, event types and the regime
periods is not measured.

Consequences, to decide before gold (step 6):

- **The per-actor funnel is not observable** as framed ("first PR, merged,
  came back"): a person's events survive at roughly 2% each, and merges carry
  no author, so attributing a merge needs the `opened` event, found ~2% of the
  time and biased to instant self-merges.
- **The recommender sees a ~2% sample of interactions.** If sampling is close
  to uniform, repo popularity survives in expectation, but per-actor histories
  are very sparse; whether enough actors have repeat interactions for kNN/ALS
  is not yet measured.
- Silver (step 3) is largely unaffected and proceeds.

## Recommender viability on the sampled data (2026-10-23)

Window 2026-09-07…10-06 (720 hours, 604 stable). Train Sep 7–29, test Sep 30 –
Oct 6. An interaction is one non-`[bot]` actor touching one repo. "Own" means
the actor's login equals the repo owner. "Evaluable" means at least one
non-own repo in train and at least one non-own repo new to that actor in test.

| Variant | Actors | Repos | Pairs | Own-repo pairs | Actors by distinct non-own repos (1 / 2 / 3–4 / 5–9 / 10+) | Repos with ≥2 non-own actors | Evaluable actors |
|---|---|---|---|---|---|---|---|
| Silver types, all hours | 975,855 | 836,911 | 1,485,344 | 29.6% | 533,184 / 84,547 / 39,483 / 15,431 / 4,273 | 109,884 | 33,553 |
| Funnel types, all hours | 1,109,482 | 913,294 | 1,728,809 | 26.7% | 631,470 / 101,396 / 48,214 / 19,097 / 5,480 | 133,638 | 40,562 |
| Funnel types, stable hours | 782,422 | 673,258 | 1,130,518 | 30.6% | 441,847 / 61,677 / 26,102 / 8,745 / 2,434 | 88,366 | 32,882 |

Reading: **the recommender is viable on GH Archive's sample.** About 33k
actors can be scored on a temporal split even with unstable hours masked, and
88k–134k repos have at least two distinct non-own actors for co-occurrence.
The matrix is extremely sparse (density ~2e-6) and about 78% of actors touch a
single non-own repo, so most users are near-cold. Limits: the test targets are
themselves a ~2% sample, so absolute Recall/NDCG understate the true values,
while the comparison between models stays fair because sampling does not
depend on the model; the 10+ bucket still contains unsuffixed bots; popularity
concentration of the test targets is not yet measured.

Direction (decided 2026-10-23): the recommender proceeds on GH Archive
(option A), with sampling stated in the evaluation. The per-actor funnel is not
observable at ~2% capture; it becomes a repo-level descriptive analysis with
the sampling caveat, and complete per-repo histories from the GitHub API
(option B) stay a possible later extension.

## Open questions

- What causes the capture regimes, and do they recur in the candidate month?
- Why does weekend volume rise in Sep 22–28?
- How many unsuffixed bots are there? The heuristics in `milestone_01.md`
  (event rate, naming patterns) are still to do.
