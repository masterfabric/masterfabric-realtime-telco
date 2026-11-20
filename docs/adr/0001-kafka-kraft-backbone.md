# 0001. Kafka in KRaft mode as the event backbone, keyed by cell

- Status: Accepted
- Date: 2026-11-21

## Context

Radio network performance counters arrive from OSS/mediation systems continuously, from thousands
of cells, with bursts after collection cycles and occasional re-sends. Several consumers need the
same feed: raw capture, aggregation, anomaly detection, and later other teams. In Cloudera CDP this
is the Streams Messaging (Kafka) service; Flume or NiFi typically feed it.

## Options

| Option | For | Against |
|---|---|---|
| **Apache Kafka 3.9, KRaft** | Industry standard log; replay by offset; consumer independence; same API as CDP Streams Messaging, Confluent, MSK, Event Hubs | One more stateful service to run |
| Kafka with ZooKeeper | Matches older CDP 7.1 clusters | ZooKeeper mode is deprecated and removed in Kafka 4.0 |
| Redpanda | Kafka API, single binary | Not Apache-licensed (BSL); not what the target estate runs |
| Read files from a landing directory | No broker | No replay by offset, no back-pressure, no multiple independent readers |

## Decision

- A single `apache/kafka:3.9.1` broker in KRaft mode (combined broker + controller), with an
  internal listener for containers and a second listener advertised on `127.0.0.1` for the host.
- `cell.kpi`: 6 partitions, 24 h retention, **message key = cell_id**. Per-cell ordering is
  preserved within a partition, and partitions are the unit of parallelism for the Spark source.
- `cell.alerts`: 3 partitions, `cleanup.policy=compact,delete`, key = cell_id, so a consumer that
  starts late can rebuild the latest alert state per cell.
- Auto topic creation is off; topics are created by an init container with explicit settings.
- The producer is idempotent (`enable.idempotence=true`, `acks=all`), zstd-compressed and batches
  with `linger.ms=20`.

## Consequences

- Keying by cell means partition sizes follow the hash of the cell IDs. In the live run the six
  partitions held between 8.3k and 11.5k messages at the same moment (see the Kafka UI screenshot).
  With thousands of cells this evens out; with few hot keys it would not.
- Replication factor 1, no TLS/SASL, PLAINTEXT listeners. All of that is a laptop simplification;
  production needs RF=3, `min.insync.replicas=2`, TLS and SASL/Kerberos (CDP) or mTLS, and ACLs per
  consumer group.

## Enterprise mapping

CDP Streams Messaging (Kafka + Schema Registry + Streams Messaging Manager). kafbat/kafka-ui stands
in for Streams Messaging Manager in this lab.
