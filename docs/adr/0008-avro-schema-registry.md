# 0008. An Avro topic with a schema registry next to the JSON contract

- Status: Accepted (extends 0007)
- Date: 2026-11-21

## Context

ADR 0007 kept JSON with an explicit Spark schema and named "Avro/Protobuf + registry" as the
production answer. That claim needed to be shown, not just stated: what the registry actually
enforces, what each engine has to do to read evolving Avro, and what it costs on the wire.

## Options

| Option | For | Against |
|---|---|---|
| Confluent Schema Registry | The reference implementation | Confluent Community License, not an OSI licence |
| Karapace | Apache-2.0, Confluent-compatible | More configuration (telemetry, leader election) for a single-node lab |
| **Apicurio Registry 3.3** | Apache-2.0, Confluent-compatible API (`/apis/ccompat/v7`), stores schemas in a Kafka journal, has a UI | A second API (v3) beside the compatible one; answers before its journal replay finishes after a restart |

## Decision

- Apicurio Registry 3.3.5 with `kafkasql` storage, behind the compose profile `schema`. Clients only
  use the Confluent-compatible API, so swapping in Confluent or Karapace is a URL change.
- Subject `cell.kpi.avro-value` (TopicNameStrategy) with a **BACKWARD** compatibility rule.
  `schemas/cell_kpi_v1.avsc` mirrors the JSON contract, `v2` adds `energy_kwh` as an optional field
  with a default, `v3_incompatible` adds a required field without a default.
- The producer writes the Confluent wire format itself (`producer/avro_codec.py`: magic byte, 4-byte
  schema id, Avro body via fastavro), so the format is visible in the code.
- Spark decodes with `from_avro` per known schema id inside one `CASE` expression and passes the latest
  version as the reader schema. Flink uses the `avro-confluent` format, which fetches the writer
  schema by id and resolves it against the table DDL.
- `rat` is a plain string, not an Avro enum: Avro cannot resolve an enum writer to the string reader
  schema Flink derives from the DDL.
- The JSON topic and the detector pipeline stay unchanged; the Avro topic feeds the engine comparison.

## Measured (`docs/results/schema_registry.json`, `engine_drain.json`)

- v2 checked compatible and registered; v3 checked incompatible and **rejected with HTTP 409**
  (`vendor at /fields/18`). Re-registering v1 returns the existing id.
- One sample record: 103 bytes Avro (5 of them header) vs 367 bytes JSON. Over the 1,042,006-record
  drain topic: 120.5 MB Avro vs 403.1 MB JSON before Kafka compression (29.9 %).
- A v1 record read with the v2 reader schema gets `energy_kwh = null`; a v3 reader fails on v1 data.

## Consequences

- Spark has no registry integration in open-source Spark: the schema ids are read once at start-up.
  A version registered while the query runs is skipped (filtered as unknown) until a restart. A first
  attempt with a union of one branch per schema id read the Kafka source twice per batch; the `CASE`
  form reads it once.
- Flink picks up new writer schemas without a restart, but its reader schema is the DDL: a new field
  is only visible after the DDL changes.
- The registry becomes a runtime dependency for producers and consumers; in production it needs HA
  and authentication, both left out here.
