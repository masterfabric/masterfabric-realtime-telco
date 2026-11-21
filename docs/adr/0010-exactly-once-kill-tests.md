# 0010. Exactly-once claims are tested with kill -9, not asserted

- Status: Accepted (verifies 0006)
- Date: 2026-11-21

## Context

ADR 0006 lists which hop is exactly-once and which is at-least-once. Those are claims about crash
behaviour, and a clean shutdown never exercises them.

## Decision

`scripts/engine_compare.py` kills processes with SIGKILL during a run, restarts them and counts:

- **Spark → Iceberg bronze** (`make eo-spark`): 521,044 JSON records, `availableNow` in batches of
  10,000 offsets, driver killed 3 times at random points inside a batch. Check (`streaming/eo_check.py`):
  table rows = distinct `(partition, offset)` = records in the topic, no offset gaps.
- **Spark → Kafka**: the same kill pattern on the Avro aggregation writing to `eo.spark.kafka`.
- **Flink → Kafka** (`make eo-flink`): two jobs on the same live feed (4 simulated hours at 60x), one
  with `sink.delivery-guarantee = exactly-once`, one `at-least-once`, checkpoints every 10 s. The
  TaskManager container is killed twice and started again after 3 s. Outputs are read with
  `read_committed` and with `read_uncommitted`.

## Measured (`docs/results/exactly_once_spark.json`, `exactly_once_flink.json`)

| Sink | Kills | Rows | Duplicates |
|---|---|---|---|
| Spark → Iceberg | 3 driver | 521,044 of 521,044 offsets, 0 gaps, 53 snapshots | **0** |
| Spark → Kafka (at-least-once by design) | 3 driver | 56,880 windows | 0 observed |
| Flink exactly-once, `read_committed` | 2 TaskManager | 56,880 windows | **0** |
| Flink exactly-once, `read_uncommitted` | | 58,320 rows | 1,440 (aborted transactions are visible) |
| Flink at-least-once | | 60,960 rows | **4,080** |

Flink noticed the lost TaskManager after 15.7 s (default heartbeat settings) and had every task running
again 18.8–18.9 s after the kill, restoring from the last checkpoint. A restarted Spark driver
committed its next batch 4.4–4.7 s after launch (one kill landed just after a commit).

## Consequences

- Exactly-once into Kafka only holds for consumers that read with `isolation.level=read_committed`;
  a default consumer sees the aborted copies.
- Spark's Kafka sink is documented as at-least-once. Three random kills did not hit the short window
  between the sink flushing and the commit log being written, so "0 observed" is not a guarantee.
  Anything downstream of a Spark Kafka sink still has to deduplicate on the key.
- The kill tests run against a local Docker volume for checkpoints; durable storage (S3, HDFS) is
  required for the same result when the machine itself fails.
