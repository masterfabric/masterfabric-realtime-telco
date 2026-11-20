"""Iceberg tables written by the streaming application (bronze, silver, gold)."""
from __future__ import annotations

# Streaming commits every few seconds create many snapshots and metadata files; keep metadata
# bounded and let maintenance (streaming/maintenance.py) compact data files and expire snapshots.
COMMON_PROPS = """
  'format-version' = '2',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20',
  'write.parquet.compression-codec' = 'zstd'
"""


def ddl(ns: str) -> list[str]:
    return [
        f"CREATE NAMESPACE IF NOT EXISTS lake.{ns}",
        f"""CREATE TABLE IF NOT EXISTS lake.{ns}.kpi_events (
              event_id STRING, event_ts TIMESTAMP, cell_id STRING, site_id STRING, city STRING, rat STRING,
              v INT, interval_s INT, active_users DOUBLE, prb_util_pct DOUBLE, dl_throughput_mbps DOUBLE,
              ul_sinr_db DOUBLE, latency_ms DOUBLE, rrc_attempts BIGINT, rrc_success BIGINT,
              erab_drops BIGINT, availability_pct DOUBLE,
              parse_ok BOOLEAN, raw STRING,
              kafka_partition INT, kafka_offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP)
            USING iceberg PARTITIONED BY (hours(event_ts))
            TBLPROPERTIES ({COMMON_PROPS})""",
        f"""CREATE TABLE IF NOT EXISTS lake.{ns}.kpi_1m (
              window_start TIMESTAMP, window_end TIMESTAMP, cell_id STRING, site_id STRING, city STRING,
              rat STRING, reports BIGINT, users_avg DOUBLE, prb_avg DOUBLE, prb_max DOUBLE,
              throughput_avg_mbps DOUBLE, sinr_avg DOUBLE, latency_avg DOUBLE, rrc_attempts BIGINT,
              rrc_success BIGINT, erab_drops BIGINT, availability_min DOUBLE,
              last_kafka_ts TIMESTAMP, emitted_at TIMESTAMP)
            USING iceberg PARTITIONED BY (hours(window_start))
            TBLPROPERTIES ({COMMON_PROPS})""",
        f"""CREATE TABLE IF NOT EXISTS lake.{ns}.alerts (
              alert_id STRING, cell_id STRING, site_id STRING, city STRING, alert_type STRING,
              status STRING, severity STRING, window_start TIMESTAMP, opened_window TIMESTAMP,
              score DOUBLE, evidence STRING, source_last_kafka_ts TIMESTAMP, detected_at TIMESTAMP,
              batch_id BIGINT)
            USING iceberg PARTITIONED BY (alert_type)
            TBLPROPERTIES ({COMMON_PROPS})""",
        f"""CREATE TABLE IF NOT EXISTS lake.{ns}.cell_health (
              cell_id STRING, site_id STRING, city STRING, state STRING, severity STRING,
              alert_id STRING, since TIMESTAMP, updated_at TIMESTAMP)
            USING iceberg
            TBLPROPERTIES ({COMMON_PROPS})""",
    ]


def drop(ns: str) -> list[str]:
    return [f"DROP TABLE IF EXISTS lake.{ns}.{t} PURGE" for t in ("kpi_events", "kpi_1m", "alerts", "cell_health")]
