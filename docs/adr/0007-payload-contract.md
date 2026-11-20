# 0007. JSON with an explicit schema contract; Avro/Protobuf + registry in production

- Status: Accepted
- Date: 2026-11-21

## Context

Producers and consumers must agree on the payload. Inferring a schema from data in a stream is
fragile, and a wrong guess silently corrupts downstream tables.

## Decision

- Payloads are JSON with a schema version field `v` (currently 1).
- Spark parses them with an **explicit** `StructType` (`streaming/schema.py`) in PERMISSIVE mode.
  A malformed payload becomes a bronze row with `parse_ok = false` and its raw text, and is excluded
  from silver. Nothing is dropped silently.
- No schema registry runs in the lab.

## Consequences

- JSON is human-readable in Kafka UI (useful for a lab and for the screenshots) but is 3-5x larger
  than Avro and has no enforced compatibility rules.
- In production: Avro or Protobuf with a schema registry (Cloudera Schema Registry or Confluent),
  `BACKWARD` compatibility, and producers that cannot publish an incompatible schema.
