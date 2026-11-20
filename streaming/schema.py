"""Explicit schemas: the Kafka payload contract and the detector's output."""
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

# Contract for cell.kpi payloads (schema version 1). Parsed with from_json in PERMISSIVE mode, so a
# malformed record becomes a row with null fields that bronze keeps (with its raw text) and silver drops.
KPI_EVENT = StructType([
    StructField("v", IntegerType()),
    StructField("event_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("cell_id", StringType()),
    StructField("site_id", StringType()),
    StructField("city", StringType()),
    StructField("rat", StringType()),
    StructField("interval_s", IntegerType()),
    StructField("active_users", DoubleType()),
    StructField("prb_util_pct", DoubleType()),
    StructField("dl_throughput_mbps", DoubleType()),
    StructField("ul_sinr_db", DoubleType()),
    StructField("latency_ms", DoubleType()),
    StructField("rrc_attempts", LongType()),
    StructField("rrc_success", LongType()),
    StructField("erab_drops", LongType()),
    StructField("availability_pct", DoubleType()),
])

ALERT = StructType([
    StructField("alert_id", StringType()),
    StructField("cell_id", StringType()),
    StructField("site_id", StringType()),
    StructField("city", StringType()),
    StructField("alert_type", StringType()),
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("window_start", TimestampType()),
    StructField("opened_window", TimestampType()),
    StructField("score", DoubleType()),
    StructField("evidence", StringType()),
    StructField("source_last_kafka_ts", TimestampType()),
])

DETECTOR_STATE = StructType([StructField("state_json", StringType())])
