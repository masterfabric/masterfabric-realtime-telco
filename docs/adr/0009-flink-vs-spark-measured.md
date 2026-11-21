# 0009. Flink vs Spark Structured Streaming, measured on the same job

- Status: Accepted (revisits 0002 with numbers)
- Date: 2026-11-21

## Context

ADR 0002 chose Spark Structured Streaming because the rest of the stack (Iceberg maintenance, the
batch lab, the pandas detector) is Spark. It did not measure what that choice costs against Flink.

## Method

The silver step was written twice over the same Avro topic: `streaming/avro_silver.py` (Spark 3.5.5,
`local[6]`, 6 shuffle partitions) and `flink/silver.sql` (Flink 2.2.1 SQL, parallelism 6, one
TaskManager with 3 GB). Both use a 2-minute watermark, drop duplicate `event_id`s and emit one row per
cell and minute to a Kafka topic. `scripts/engine_compare.py` drives the runs and compares the outputs
window by window.

## Measured (`docs/results/engine_drain.json`, `engine_live.json`)

| | Spark | Flink |
|---|---|---|
| Drain 1,042,006 records, median of 3 (processing) | 10.54 s · 98,862 rec/s | 3.07 s · 339,416 rec/s |
| Same, including JVM / job start | 12.33 s | 7.79 s |
| Live at 1,449 events/s, latency p50 / p95 / p99 | 4.06 / 4.35 / 4.47 s | 2.60 / 2.71 / 2.74 s (at-least-once) |
| Flink exactly-once sink (checkpoint 10 s) | | 7.72 / 11.75 / 11.88 s |
| CPU while live (docker stats average) | 83 % (whole Spark container) | 12 % (TaskManager running 2 jobs) |
| Windows identical to the other engine | 28,080 / 28,080 live · 172,080 / 172,080 drain | |

The 2-minute watermark alone is 2 s of wall time at 60x. Flink sits 0.6 s above it, Spark about 2 s
(one trigger to see the data and advance the watermark, the next to emit). Flink's exactly-once rows
only become visible to `read_committed` readers when the checkpoint commits the Kafka transaction.

## Decision

- Keep Spark for the main pipeline: it writes Iceberg, runs the pandas detector with state and shares
  code with the batch lab. Its numbers here are more than enough for 240 cells.
- Choose Flink where a job is Kafka to Kafka, latency below a few seconds matters, or many such jobs
  share a cluster: it used about a seventh of the CPU on the same feed.
- If Flink is used with an exactly-once Kafka sink, the checkpoint interval sets the visible latency.

## Limits of the comparison

- One laptop, one run of the live test, both engines sharing 6 vCPUs while live. The Spark container
  also runs a Thrift server, a history server and Jupyter (idle: 0.3 % CPU, 768 MiB).
- On a backlog neither engine dropped a late record: the watermark only moves per micro-batch (Spark)
  or every 200 ms (Flink), so the drain test says nothing about lateness. In the live run both dropped
  the same 188 very late reports, and both outputs match a replay of the input with a global 2-minute
  watermark exactly.
- Spark's `availableNow` run does not flush the last 3 open minutes (720 windows); a bounded Flink job
  closes them at the end of input.
- Spark's Kafka sink and its RocksDB / HDFS state store choices, Flink's state backend (HashMap here)
  and the trigger interval all move these numbers; none were tuned.
