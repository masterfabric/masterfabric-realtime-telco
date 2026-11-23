# 0003. Event time, a 2-minute watermark and dedupe within the watermark

- Status: Accepted
- Date: 2026-11-21 (evidence added 2026-11-23)

## Context

The producer deliberately delivers like a real mediation chain: about 3% of reports arrive 5-60
seconds late and out of order, about 0.1% arrive 5-10 minutes late, and about 0.5% are re-sent
with the same `event_id`. Windowing by arrival time would put a report in the wrong minute;
keeping every window open forever would grow state without bound.

## Decision

- All windows are on **event time** (`event_ts` from the payload), 1-minute tumbling windows.
- **Watermark = max event time seen - 2 minutes.** Windows are emitted in append mode only once
  the watermark passes them, so silver never contains a partial minute that is later revised.
- **`dropDuplicatesWithinWatermark(event_id)`** (Spark 3.5) removes re-sends with state bounded by
  the same watermark, instead of keeping every ID ever seen.
- Bronze does **not** apply the watermark: it keeps every delivered record, duplicates and very
  late ones included, so the raw truth of what arrived is auditable and replayable.

## Consequences

- Reports later than the watermark are dropped from silver (they remain in bronze). The evaluator
  measures the gap as `bronze unique events - silver reports`.
- The last two simulated minutes of a run are never emitted, because the watermark never passes
  them once input stops. The evaluator excludes incidents that start in that tail.
- Watermarks advance per micro-batch. With a simulated clock, the trigger interval in simulated
  time is `trigger seconds x speed`; at the 120x smoke-test speed a 5-second trigger spans
  10 simulated minutes, so even the "very late" reports landed in time. The live run therefore uses
  30x and a 2-second silver trigger (1 simulated minute).

## Evidence (2026-11-23)

`make watermark-sweep` replays one recorded delivery (seed 2026, 240 cells, 2 simulated hours,
173,701 reports, cut into 30-second arrival batches) through the silver code once per watermark
delay (`docs/results/watermark_sweep.json`):

| Watermark | Reports lost | Windows incomplete | Window end → emitted |
|---|---:|---:|---:|
| 10 s | 2,999 | 9.95 % | 0.5 min |
| 30 s | 1,087 | 3.70 % | 1.0 min |
| 1 min | 190 | 0.66 % | 1.5 min |
| **2 min (chosen)** | **188** | **0.65 %** | **2.5 min** |
| 5 min | 178 | 0.62 % | 5.5 min |
| 11 min | 0 | 0 % | 11.5 min |

Ordinary lateness (5 to 60 s) is fully absorbed from 1 minute on; everything lost at 1 to 5 minutes
is the very late tail (300 to 600 s), which only an 11-minute watermark keeps, at 4.6 times the
delay. 2 minutes keeps one minute of margin over the observed lateness for one extra minute of
delay. `make test-spark` pins the semantics: out-of-order reports land in their window, a report
behind the watermark is dropped and never reopens an emitted window, a re-send is counted once, and
the watermark is global, so a cell whose clock runs 3 minutes behind loses all its reports.

That last point is a real operational risk: one watermark per query means a source with a skewed
clock is silently discarded. Clock skew per cell should be monitored in bronze (event time against
Kafka time), not discovered as missing silver rows.

## Enterprise mapping

The same semantics exist in Flink (bounded out-of-orderness watermarks) and in Kafka Streams
(grace periods). The right lateness bound is a business decision: how long NOC dashboards may
show a minute as incomplete.
