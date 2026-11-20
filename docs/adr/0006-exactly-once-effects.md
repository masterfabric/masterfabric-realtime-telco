# 0006. Exactly-once effects: checkpoints, idempotent MERGE, keyed at-least-once alerts

- Status: Accepted
- Date: 2026-11-21

## Context

A restart must not lose events, double-count windows or raise the same alert twice in the tables.

## Decision

| Hop | Guarantee | Mechanism |
|---|---|---|
| Producer -> Kafka | No duplicates from producer retries | Idempotent producer, `acks=all` |
| Mediation re-sends | Removed in silver | `dropDuplicatesWithinWatermark(event_id)` |
| Kafka -> bronze / silver (Iceberg) | Exactly-once | Offsets in the query checkpoint; the Iceberg sink commits a micro-batch once per batch ID |
| silver -> detector state | Exactly-once state updates | Iceberg source offsets + RocksDB state in the checkpoint; windows at or before the state's `last_window` are skipped |
| detector -> `rt.alerts`, `rt.cell_health` | Effectively exactly-once | `foreachBatch` is at-least-once, so both writes are `MERGE` statements keyed on `(alert_id, status)` and `cell_id` |
| detector -> Kafka `cell.alerts` | At-least-once | Keyed by cell, carries `alert_id` + `status`; consumers deduplicate, the compacted topic converges |

## Consequences

- Checkpoints live on a local Docker volume. In production they belong on HDFS or S3 (s3a) with the
  same durability as the tables.
- Kafka transactions for the alert topic were not used: the downstream consumers (ticketing) already
  need idempotency on `alert_id`, and transactions would couple the Spark batch to the broker.
