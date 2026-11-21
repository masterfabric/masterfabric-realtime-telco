# Architecture decision records

Each record states the context, the options that were weighed, the decision and its consequences.
They are written for a reader who knows the Cloudera / Hadoop world and wants to see where this lab
deliberately differs and why.

| # | Decision |
|---|---|
| [0001](0001-kafka-kraft-backbone.md) | Kafka in KRaft mode as the event backbone, keyed by cell |
| [0002](0002-spark-structured-streaming.md) | Spark Structured Streaming rather than Flink for this lab |
| [0003](0003-event-time-watermarks.md) | Event time, a 2-minute watermark and dedupe within the watermark |
| [0004](0004-iceberg-between-stages.md) | Iceberg tables, not topics, as the hand-off between streaming stages |
| [0005](0005-adaptive-detector.md) | Per-cell adaptive baselines and a Bernoulli CUSUM instead of a trained model |
| [0006](0006-exactly-once-effects.md) | Exactly-once effects: checkpoints, idempotent MERGE, keyed at-least-once alerts |
| [0007](0007-payload-contract.md) | JSON with an explicit schema contract; Avro/Protobuf + registry in production |
| [0008](0008-avro-schema-registry.md) | An Avro topic with a schema registry (Apicurio, BACKWARD rule) next to the JSON contract |
| [0009](0009-flink-vs-spark-measured.md) | Flink vs Spark Structured Streaming, measured on the same job |
| [0010](0010-exactly-once-kill-tests.md) | Exactly-once claims are tested with kill -9, not asserted |
