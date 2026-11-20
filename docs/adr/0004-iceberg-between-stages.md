# 0004. Iceberg tables, not topics, as the hand-off between streaming stages

- Status: Accepted
- Date: 2026-11-21

## Context

Detection needs finalised 1-minute windows. The windows could be passed on through another Kafka
topic, kept inside one big query, or written to a table that the detector reads as a stream.

## Decision

- Bronze and silver are **independent queries** on the Kafka source with their own checkpoints, so
  a slow aggregation never delays raw capture, and either can be restarted alone.
- Silver writes finalised windows to the Iceberg table `rt.kpi_1m`. The detector reads that table
  as a **streaming Iceberg source** and keeps its own checkpoint.
- Alerts go to the Iceberg table `rt.alerts` **and** to the Kafka topic `cell.alerts`: the table for
  analysis and audit, the topic for systems that react (ticketing, NOC screens).

## Consequences

- The contract between aggregation and detection is a table: it can be queried by any engine,
  time-travelled, replayed into a new detector version, and backfilled by batch jobs.
- Each stage adds a commit interval. In the live run most of the alert latency comes from the
  silver stage (watermark wait + trigger) and the detector trigger; the numbers are in the README.
- Streaming commits create many small files and snapshots. The tables set
  `write.metadata.delete-after-commit.enabled` and `previous-versions-max=20`, and
  `streaming/maintenance.py` compacts data files and expires snapshots after a run.
- The Iceberg streaming source stores its offsets through the table's FileIO; with local
  checkpoints the catalog uses `ResolvingFileIO` (S3 for data, local files for offsets).

## Alternatives considered

- **A Kafka topic for windows**: lower latency, but no SQL access, retention-bound replay and a second
  schema to govern.
- **One query with aggregation and detection chained**: fewer moving parts, but two stateful
  operators with different state shapes in one checkpoint, and no reusable silver table.
