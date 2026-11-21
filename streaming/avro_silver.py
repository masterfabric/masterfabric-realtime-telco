"""Spark side of the engine comparison: the silver aggregation over the Avro topic, written to Kafka.

    Kafka cell.kpi.avro (Confluent wire format, schema v1 and v2 mixed)
        ──► split by schema id, from_avro(writer schema, reader schema = latest)
        ──► watermark 2 min, dedupe event_id within the watermark, 1-minute windows per cell
        ──► Kafka kpi_1m.spark (JSON)

Spark has no built-in schema-registry lookup, so writer schemas are fetched once at start-up and
each id gets its own from_avro call; the latest version is passed as the reader schema (`avroSchema`)
so v1 rows come out with energy_kwh = null. A schema id registered after start-up is not decoded
until the query restarts (counted, not silently mixed in). The Kafka sink is at-least-once.

    spark-submit streaming/avro_silver.py --available-now           # drain a pre-filled topic
    spark-submit streaming/avro_silver.py --trigger-s 1 --idle-stop-s 30   # live
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from producer.avro_codec import Registry  # noqa: E402

AGG_COLS = ["window_start", "cell_id", "reports", "users_avg", "prb_avg", "prb_max", "throughput_avg_mbps",
            "latency_avg", "rrc_attempts", "rrc_success", "erab_drops", "availability_min", "energy_kwh_sum",
            "v2_reports"]


def writer_schemas(reg: Registry, subject: str) -> dict[int, str]:
    _, versions = reg.versions(subject)
    out = {}
    for v in versions:
        _, body = reg._call("GET", f"/subjects/{subject}/versions/{v}")
        out[body["id"]] = body["schema"]
    return out


def build(spark: SparkSession, a: argparse.Namespace):
    reg = Registry(a.registry)
    schemas = writer_schemas(reg, f"{a.topic}-value")
    reader = schemas[max(schemas)]
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", a.max_offsets)
           .option("failOnDataLoss", "false")
           .load()
           .withColumn("schema_id", F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int"))
           .withColumn("body", F.expr("substring(value, 6, length(value) - 5)")))
    # One CASE over the schema id instead of a union of filtered branches: a union would scan the
    # Kafka source once per schema id. Every branch yields the same struct (the reader schema).
    decoded = None
    for sid, ws in sorted(schemas.items()):
        expr = from_avro("body", ws, {"avroSchema": reader, "mode": "FAILFAST"})
        decoded = F.when(F.col("schema_id") == sid, expr) if decoded is None else decoded.when(
            F.col("schema_id") == sid, expr)
    src = (raw.select(decoded.alias("r"), F.col("timestamp").alias("kafka_ts"), "schema_id")
              .filter(F.col("r").isNotNull())          # unknown schema id (registered after start-up)
              .select("r.*", "kafka_ts"))
    agg = (src.withWatermark("event_ts", f"{a.watermark_min} minutes")
              .dropDuplicatesWithinWatermark(["event_id"])
              .groupBy(F.window("event_ts", "1 minute"), "cell_id")
              .agg(F.count("*").alias("reports"),
                   F.avg("active_users").alias("users_avg"),
                   F.avg("prb_util_pct").alias("prb_avg"),
                   F.max("prb_util_pct").alias("prb_max"),
                   F.avg("dl_throughput_mbps").alias("throughput_avg_mbps"),
                   F.avg("latency_ms").alias("latency_avg"),
                   F.sum("rrc_attempts").alias("rrc_attempts"),
                   F.sum("rrc_success").alias("rrc_success"),
                   F.sum("erab_drops").alias("erab_drops"),
                   F.min("availability_pct").alias("availability_min"),
                   F.sum("energy_kwh").alias("energy_kwh_sum"),
                   F.sum(F.when(F.col("v") == 2, 1).otherwise(0)).alias("v2_reports"))
              .withColumn("window_start", F.date_format("window.start", "yyyy-MM-dd HH:mm:ss")))
    out = agg.select(F.concat_ws("|", "cell_id", "window_start").alias("key"),
                     F.to_json(F.struct(*AGG_COLS)).alias("value"))
    trig = {"availableNow": True} if a.available_now else {"processingTime": f"{a.trigger_s} seconds"}
    return (out.writeStream.queryName("avro_silver").format("kafka").outputMode("append").trigger(**trig)
               .option("kafka.bootstrap.servers", a.bootstrap)
               .option("topic", a.out_topic)
               .option("checkpointLocation", a.checkpoint)
               .start()), sorted(schemas)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", default="cell.kpi.avro")
    p.add_argument("--out-topic", default="kpi_1m.spark")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://registry:8080/apis/ccompat/v7"))
    p.add_argument("--checkpoint", default="/checkpoints/avro_silver")
    p.add_argument("--max-offsets", type=int, default=200000)
    p.add_argument("--watermark-min", type=int, default=2)
    p.add_argument("--trigger-s", type=int, default=1)
    p.add_argument("--available-now", action="store_true")
    p.add_argument("--idle-stop-s", type=int, default=30)
    p.add_argument("--timeout-s", type=int, default=3600)
    p.add_argument("--reset", action="store_true", help="delete the checkpoint first")
    p.add_argument("--timing-out", default=None, help="write a JSON with wall time and input rows")
    a = p.parse_args()
    if a.reset:
        shutil.rmtree(a.checkpoint, ignore_errors=True)
    spark = (SparkSession.builder.appName("mf-avro-silver")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "6"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()
    q, ids = build(spark, a)
    rows, last_change = 0, time.time()
    while q.isActive:
        q.awaitTermination(2)
        prog = q.recentProgress
        n = sum(int(x.get("numInputRows", 0)) for x in prog) if prog else 0
        if n != rows:
            rows, last_change = n, time.time()
        if not a.available_now and time.time() - last_change > a.idle_stop_s:
            q.stop()
        if time.time() - t0 > a.timeout_s:
            q.stop()
    wall = time.time() - t0
    rows = sum(int(x.get("numInputRows", 0)) for x in q.recentProgress)  # recentProgress keeps the last 100
    res = {"engine": "spark", "wall_s": round(wall, 2), "schema_ids": ids, "batches": len(q.recentProgress),
           "input_rows_recent": rows, "spark": spark.version}
    print(json.dumps(res))
    if a.timing_out:
        with open(a.timing_out, "w") as f:
            json.dump(res, f, indent=2)


if __name__ == "__main__":
    main()
