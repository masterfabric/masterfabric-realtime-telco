# 0002. Spark Structured Streaming rather than Flink for this lab

- Status: Accepted
- Date: 2026-11-21

## Context

The stream processor has to: parse and validate events, handle event time with late and duplicate
data, aggregate per cell and minute, keep per-cell state for detection, and write to Iceberg with
exactly-once effects. Cloudera offers both Spark (Data Engineering) and Flink (Streaming Analytics).

## Options

| Option | For | Against |
|---|---|---|
| **Spark Structured Streaming 3.5** | One engine and one skill set for batch (see the lakehouse lab) and streaming; first-class Iceberg sink and source; `applyInPandasWithState` for arbitrary per-key state in Python; RocksDB state store | Micro-batch: end-to-end latency is seconds, not milliseconds |
| Apache Flink 1.20 | True record-at-a-time processing, millisecond latency, rich timers and state TTL | A second engine to operate; PyFlink state APIs are less mature than Java; heavier on a 24 GB laptop next to Kafka, Spark and storage |
| Kafka Streams | Lightweight, library not cluster | JVM only, no Iceberg sink, state tied to Kafka topics |

## Decision

Spark Structured Streaming 3.5.5, local mode with 4 cores inside the container, RocksDB state store,
6 shuffle partitions (one per Kafka partition). Three queries in one driver (ADR 0004).

## Consequences

- Network-alarm latency here is "seconds", which matches how NOC alarms are consumed (people and
  ticketing systems), but not sub-second use cases such as real-time charging or fraud blocking,
  where Flink is the better choice.
- Detection logic is plain Python (`streaming/detector.py`) and runs unchanged offline with pandas
  (`streaming/offline.py`), which made tuning and testing cheap.
- Watermarks only advance between micro-batches. With a simulated clock this matters: at 30x
  speed a 2-second trigger spans one simulated minute. ADR 0003 discusses it.

## Enterprise mapping

CDP Data Engineering / Data Hub Spark 3, or Cloudera Streaming Analytics (Flink) for lower latency.
