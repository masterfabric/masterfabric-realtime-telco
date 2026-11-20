# 0003. Event time, a 2-minute watermark and dedupe within the watermark

- Status: Accepted
- Date: 2026-11-21

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

## Enterprise mapping

The same semantics exist in Flink (bounded out-of-orderness watermarks) and in Kafka Streams
(grace periods). The right lateness bound is a business decision: how long NOC dashboards may
show a minute as incomplete.
