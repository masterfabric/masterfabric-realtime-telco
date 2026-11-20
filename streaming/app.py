"""The streaming application: three Structured Streaming queries chained through Iceberg.

    Kafka cell.kpi ──► [bronze]  parse, keep everything (incl. duplicates, late, malformed) ──► rt.kpi_events
                  └──► [silver]  watermark 2 min, dedupe by event_id, 1-minute windows   ──► rt.kpi_1m
    rt.kpi_1m (Iceberg stream) ──► [detect] per-cell state (applyInPandasWithState)
                                   ──► foreachBatch: MERGE rt.alerts, MERGE rt.cell_health, Kafka cell.alerts

Bronze and silver read Kafka independently with their own checkpoints, so a slow aggregation never
holds back raw capture. The detector reads silver as an Iceberg stream: the only contract between
the two is a table, which can be replayed, time-travelled and queried by other engines.

    spark-submit streaming/app.py --run-id live --reset
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from streaming import detector, tables
from streaming.schema import ALERT, DETECTOR_STATE, KPI_EVENT


def kafka_source(spark: SparkSession, a: argparse.Namespace) -> DataFrame:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", a.bootstrap)
           .option("subscribe", a.topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", a.max_offsets)
           .option("failOnDataLoss", "false")
           .load())
    j = F.from_json(F.col("value").cast("string"), KPI_EVENT, {"mode": "PERMISSIVE"})
    return (raw.select(F.col("value").cast("string").alias("raw"), j.alias("j"),
                       F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
                       F.col("timestamp").alias("kafka_ts"))
               .select("j.*", "raw", "kafka_partition", "kafka_offset", "kafka_ts")
               .withColumn("event_ts", F.to_timestamp("event_ts"))
               .withColumn("parse_ok", F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()))


def trigger(a: argparse.Namespace, seconds: int) -> dict:
    """Live runs use processing-time triggers; benchmarks drain the topic with availableNow."""
    return {"availableNow": True} if a.available_now else {"processingTime": f"{seconds} seconds"}


def bronze_query(src: DataFrame, a: argparse.Namespace):
    out = (src.withColumn("raw", F.when(F.col("parse_ok"), F.lit(None)).otherwise(F.col("raw")))
              .withColumn("ingested_at", F.current_timestamp())
              .select("event_id", "event_ts", "cell_id", "site_id", "city", "rat", "v", "interval_s",
                      "active_users", "prb_util_pct", "dl_throughput_mbps", "ul_sinr_db", "latency_ms",
                      "rrc_attempts", "rrc_success", "erab_drops", "availability_pct", "parse_ok", "raw",
                      "kafka_partition", "kafka_offset", "kafka_ts", "ingested_at"))
    return (out.writeStream.queryName("bronze").format("iceberg").outputMode("append")
               .trigger(**trigger(a, a.trigger_bronze))
               .option("checkpointLocation", f"{a.checkpoints}/bronze")
               .option("fanout-enabled", "true")
               .toTable(f"lake.{a.ns}.kpi_events"))


def silver_query(src: DataFrame, a: argparse.Namespace):
    agg = (src.filter("parse_ok")
              .withWatermark("event_ts", f"{a.watermark_min} minutes")
              .dropDuplicatesWithinWatermark(["event_id"])
              .groupBy(F.window("event_ts", "1 minute"), "cell_id", "site_id", "city", "rat")
              .agg(F.count("*").alias("reports"),
                   F.avg("active_users").alias("users_avg"),
                   F.avg("prb_util_pct").alias("prb_avg"),
                   F.max("prb_util_pct").alias("prb_max"),
                   F.avg("dl_throughput_mbps").alias("throughput_avg_mbps"),
                   F.avg("ul_sinr_db").alias("sinr_avg"),
                   F.avg("latency_ms").alias("latency_avg"),
                   F.sum("rrc_attempts").alias("rrc_attempts"),
                   F.sum("rrc_success").alias("rrc_success"),
                   F.sum("erab_drops").alias("erab_drops"),
                   F.min("availability_pct").alias("availability_min"),
                   F.max("kafka_ts").alias("last_kafka_ts"))
              .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      "cell_id", "site_id", "city", "rat", "reports", "users_avg", "prb_avg", "prb_max",
                      "throughput_avg_mbps", "sinr_avg", "latency_avg", "rrc_attempts", "rrc_success",
                      "erab_drops", "availability_min", "last_kafka_ts")
              .withColumn("emitted_at", F.current_timestamp()))
    return (agg.writeStream.queryName("silver").format("iceberg").outputMode("append")
               .trigger(**trigger(a, a.trigger_silver))
               .option("checkpointLocation", f"{a.checkpoints}/silver")
               .option("fanout-enabled", "true")
               .toTable(f"lake.{a.ns}.kpi_1m"))


def detect_cell(key: tuple, pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    """applyInPandasWithState body: one call per cell per micro-batch, state = detector JSON."""
    st = detector.CellState.from_json(state.get[0] if state.exists else None)
    rows = pd.concat(list(pdfs)).sort_values("window_start")
    out = []
    for w in rows.to_dict("records"):
        ws = str(pd.Timestamp(w["window_start"]))
        if st.last_window is not None and ws <= st.last_window:
            continue                                # replayed window after a restart: already seen
        w["window_start"] = ws
        for e in detector.step(st, w):
            e.update(site_id=w["site_id"], city=w["city"], source_last_kafka_ts=w["last_kafka_ts"])
            out.append(e)
    state.update((st.to_json(),))
    if out:
        df = pd.DataFrame(out)
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["opened_window"] = pd.to_datetime(df["opened_window"])
        yield df[[f.name for f in ALERT.fields]]


def make_alert_sink(spark: SparkSession, a: argparse.Namespace):
    def sink(batch: DataFrame, batch_id: int) -> None:
        b = batch.withColumn("detected_at", F.current_timestamp()).withColumn("batch_id", F.lit(batch_id)).persist()
        if b.isEmpty():
            b.unpersist()
            return
        b.createOrReplaceTempView("new_alerts")
        ss = b.sparkSession            # the view lives in the batch's (cloned) session
        # Idempotent: a replayed batch (foreachBatch is at-least-once) inserts nothing twice.
        ss.sql(f"""MERGE INTO lake.{a.ns}.alerts t USING new_alerts s
                      ON t.alert_id = s.alert_id AND t.status = s.status
                      WHEN NOT MATCHED THEN INSERT *""")
        ss.sql(f"""MERGE INTO lake.{a.ns}.cell_health t
                      USING (SELECT cell_id, site_id, city,
                                    CASE WHEN status = 'OPEN' THEN alert_type ELSE 'ok' END AS state,
                                    CASE WHEN status = 'OPEN' THEN severity END AS severity,
                                    CASE WHEN status = 'OPEN' THEN alert_id END AS alert_id,
                                    window_start AS since, detected_at AS updated_at
                             FROM (SELECT *, row_number() OVER (PARTITION BY cell_id
                                          ORDER BY window_start DESC, status DESC) AS rn FROM new_alerts)
                             WHERE rn = 1) s
                      ON t.cell_id = s.cell_id
                      WHEN MATCHED AND s.since >= t.since THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")
        (b.select(F.col("cell_id").alias("key"),
                  F.to_json(F.struct(*[c for c in b.columns if c != "evidence"],
                                     F.from_json("evidence", "map<string,double>").alias("evidence"))).alias("value"))
          .write.format("kafka").option("kafka.bootstrap.servers", a.bootstrap)
          .option("topic", a.alerts_topic).save())
        b.unpersist()
    return sink


def detect_query(spark: SparkSession, a: argparse.Namespace):
    silver = spark.readStream.format("iceberg").load(f"lake.{a.ns}.kpi_1m")
    alerts = silver.groupBy("cell_id").applyInPandasWithState(
        detect_cell, ALERT, DETECTOR_STATE, "append", GroupStateTimeout.NoTimeout)
    return (alerts.writeStream.queryName("detect").outputMode("append")
                  .trigger(processingTime=f"{a.trigger_detect} seconds")
                  .option("checkpointLocation", f"{a.checkpoints}/detect")
                  .foreachBatch(make_alert_sink(spark, a)).start())


def progress_row(p: dict) -> dict:
    ops = p.get("stateOperators") or []
    return {
        "batch": p["batchId"], "timestamp": p["timestamp"], "rows": p["numInputRows"],
        "input_rps": round(p.get("inputRowsPerSecond") or 0.0, 1),
        "processed_rps": round(p.get("processedRowsPerSecond") or 0.0, 1),
        "trigger_ms": p["durationMs"].get("triggerExecution"), "add_batch_ms": p["durationMs"].get("addBatch"),
        "state_rows": sum(o.get("numRowsTotal", 0) for o in ops),
        "state_mem_bytes": sum(o.get("memoryUsedBytes", 0) for o in ops),
        "dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
        "watermark": (p.get("eventTime") or {}).get("watermark"),
        "sink_rows": (p.get("sink") or {}).get("numOutputRows"),
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", required=True)
    p.add_argument("--ns", default="rt")
    p.add_argument("--topic", default="cell.kpi")
    p.add_argument("--alerts-topic", default="cell.alerts")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--max-offsets", type=int, default=100000)
    p.add_argument("--watermark-min", type=int, default=2)
    p.add_argument("--trigger-bronze", type=int, default=10)
    p.add_argument("--trigger-silver", type=int, default=5)
    p.add_argument("--trigger-detect", type=int, default=5)
    p.add_argument("--queries", default="bronze,silver,detect")
    p.add_argument("--available-now", action="store_true", help="drain what is in Kafka and stop (benchmarks)")
    p.add_argument("--idle-stop-s", type=int, default=40, help="stop once the producer is done and input is idle")
    p.add_argument("--timeout-s", type=int, default=3600)
    p.add_argument("--runs-dir", default="/data/runs")
    p.add_argument("--reset", action="store_true", help="drop the namespace tables and checkpoints first")
    a = p.parse_args()
    a.checkpoints = f"/checkpoints/{a.ns}/{a.run_id}"

    spark = SparkSession.builder.appName(f"mf-realtime-telco[{a.run_id}]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if a.reset:
        for s in tables.drop(a.ns):
            spark.sql(s)
        shutil.rmtree(a.checkpoints, ignore_errors=True)
    for s in tables.ddl(a.ns):
        spark.sql(s)

    wanted = a.queries.split(",")
    src = kafka_source(spark, a)
    queries = []
    if "bronze" in wanted:
        queries.append(bronze_query(src, a))
    if "silver" in wanted:
        queries.append(silver_query(src, a))
    if "detect" in wanted:
        queries.append(detect_query(spark, a))

    run_dir = Path(a.runs_dir) / a.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    print("QUERIES_STARTED", [q.name for q in queries], flush=True)
    progress: dict[str, list[dict]] = {q.name: [] for q in queries}
    seen: dict[str, int] = {}
    started = time.time()
    idle_since = None
    while True:
        time.sleep(2)
        for q in queries:
            for pr in q.recentProgress:
                if pr["batchId"] > seen.get(q.name, -1):
                    seen[q.name] = pr["batchId"]
                    progress[q.name].append(progress_row(pr))
            if q.exception() is not None:
                raise RuntimeError(f"query {q.name} failed: {q.exception()}")
        if a.available_now and all(not q.isActive for q in queries):
            for q in queries:                       # pick up the final progress events
                for pr in q.recentProgress:
                    if pr["batchId"] > seen.get(q.name, -1):
                        seen[q.name] = pr["batchId"]
                        progress[q.name].append(progress_row(pr))
            break
        busy = any(q.status["isDataAvailable"] or (q.lastProgress or {}).get("numInputRows", 0) > 0 for q in queries)
        producer_done = (run_dir / "manifest.json").exists()
        if producer_done and not busy:
            idle_since = idle_since or time.time()
            if time.time() - idle_since >= a.idle_stop_s:
                break
        else:
            idle_since = None
        if time.time() - started > a.timeout_s:
            break
    for q in queries:
        q.stop()
    (run_dir / f"progress_{a.ns}.json").write_text(json.dumps(
        {"run_id": a.run_id, "ns": a.ns, "wall_seconds": round(time.time() - started, 1),
         "config": {k: v for k, v in vars(a).items() if k != "bootstrap"}, "queries": progress}, indent=1))
    print(json.dumps({q: len(v) for q, v in progress.items()}))
    spark.stop()


if __name__ == "__main__":
    main()
