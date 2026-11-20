"""Evaluate a finished live run: reconcile counts, score alerts against ground truth, measure latency.

    spark-submit streaming/evaluate.py --run-id live --ns rt --out docs/results/live_run.json
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from streaming import detector
from streaming.scoring import score


def pct(spark: SparkSession, sql: str) -> dict:
    r = spark.sql(sql).collect()[0]
    return {k: (round(float(v), 3) if v is not None else None) for k, v in r.asDict().items()}


def latency(spark: SparkSession, table: str, end: str, start: str, where: str = "true") -> dict:
    expr = f"(unix_millis({end}) - unix_millis({start})) / 1000.0"
    return pct(spark, f"""SELECT percentile({expr}, 0.5) AS p50_s, percentile({expr}, 0.95) AS p95_s,
                                 percentile({expr}, 0.99) AS p99_s, max({expr}) AS max_s, count(*) AS n
                          FROM {table} WHERE {where}""")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--ns", default="rt")
    p.add_argument("--runs-dir", default="/data/runs")
    p.add_argument("--out", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName(f"mf-realtime-evaluate[{a.run_id}]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    rd = Path(a.runs_dir) / a.run_id
    man = json.loads((rd / "manifest.json").read_text())
    truth = json.loads((rd / "truth.json").read_text())
    prog = json.loads((rd / f"progress_{a.ns}.json").read_text())
    ns = f"lake.{a.ns}"

    b = spark.sql(f"""SELECT count(*) rows, count(DISTINCT event_id) unique_events,
                             sum(CASE WHEN parse_ok THEN 0 ELSE 1 END) parse_failures FROM {ns}.kpi_events""").collect()[0]
    s = spark.sql(f"SELECT count(*) windows, sum(reports) reports, count(DISTINCT cell_id) cells FROM {ns}.kpi_1m").collect()[0]
    recon = {
        "producer_sent": man["delivery"]["sent"], "producer_reports": man["delivery"]["reports"],
        "bronze_rows": b["rows"], "bronze_unique_events": b["unique_events"], "bronze_parse_failures": b["parse_failures"],
        "silver_windows": s["windows"], "silver_reports": s["reports"], "silver_cells": s["cells"],
        "duplicates_removed": b["rows"] - b["unique_events"],
        "not_in_silver": b["unique_events"] - s["reports"],
        "producer_very_late": man["delivery"]["very_late"],
    }

    opens = spark.sql(f"SELECT cell_id, alert_type, window_start FROM {ns}.alerts WHERE status = 'OPEN'").toPandas()
    sim_start = datetime.fromisoformat(man["sim_start"])
    quality = score(opens, truth, sim_start, man["sim_minutes"])

    # Same silver windows through the static-threshold rule set, for an apples-to-apples comparison.
    w = spark.sql(f"SELECT * FROM {ns}.kpi_1m ORDER BY cell_id, window_start").toPandas()
    static_opens, prev = [], {}
    for r in w.to_dict("records"):
        k = detector.static_rules(r)
        if k and k != prev.get(r["cell_id"]):
            static_opens.append({"cell_id": r["cell_id"], "alert_type": k, "window_start": r["window_start"]})
        prev[r["cell_id"]] = k
    import pandas as pd
    static_quality = score(pd.DataFrame(static_opens), truth, sim_start, man["sim_minutes"])

    # KPI traces around one incident of each kind (plus a flash crowd) for the README charts.
    alerts_pd = spark.sql(f"SELECT cell_id, alert_type, status, window_start, detected_at FROM {ns}.alerts").toPandas()
    picks, seen_kinds = [], set()
    for i in truth["incidents"] + truth["benign"]:
        if i["kind"] not in seen_kinds and i["start_min"] > 90 and i["end_min"] < man["sim_minutes"] - 30:
            seen_kinds.add(i["kind"])
            picks.append(i)
    traces = []
    for i in picks:
        t = w[w.cell_id == i["cell_id"]].copy()
        t["minute"] = (pd.to_datetime(t.window_start).dt.tz_localize(None) - pd.Timestamp(sim_start).tz_localize(None)).dt.total_seconds() / 60
        t = t[(t.minute >= i["start_min"] - 45) & (t.minute <= i["end_min"] + 30)]
        traces.append({"incident": i, "minute": t.minute.round(1).tolist(),
                       **{c: t[c].round(4).tolist() for c in ("prb_avg", "latency_avg", "sinr_avg", "availability_min")},
                       "rrc_sr": (t.rrc_success / t.rrc_attempts.clip(lower=1)).round(4).tolist(),
                       "alerts": [{"type": r.alert_type, "status": r.status,
                                   "minute": (pd.Timestamp(r.window_start).tz_localize(None) - pd.Timestamp(sim_start).tz_localize(None)).total_seconds() / 60}
                                  for r in alerts_pd[alerts_pd.cell_id == i["cell_id"]].itertuples()]})
    Path(a.out).with_name(Path(a.out).stem + "_traces.json").write_text(json.dumps(traces, default=float))

    lat_raw = spark.sql(f"""SELECT (unix_millis(detected_at) - unix_millis(source_last_kafka_ts)) / 1000.0 AS s
                            FROM {ns}.alerts WHERE status = 'OPEN'""").toPandas().s.round(3).tolist()
    silver_raw = spark.sql(f"""SELECT (unix_millis(emitted_at) - unix_millis(last_kafka_ts)) / 1000.0 AS s
                               FROM {ns}.kpi_1m""").toPandas().s.round(3).tolist()
    Path(a.out).with_name(Path(a.out).stem + "_latency.json").write_text(json.dumps({"alert": lat_raw, "silver": silver_raw}, default=float))

    lat = {
        "kafka_to_bronze": latency(spark, f"{ns}.kpi_events", "ingested_at", "kafka_ts"),
        "last_report_to_silver_window": latency(spark, f"{ns}.kpi_1m", "emitted_at", "last_kafka_ts"),
        "last_report_to_alert": latency(spark, f"{ns}.alerts", "detected_at", "source_last_kafka_ts", "status = 'OPEN'"),
    }

    queries = {}
    for q, rows in prog["queries"].items():
        active = [r for r in rows if r["rows"] > 0]
        queries[q] = {
            "batches": len(rows), "batches_with_data": len(active),
            "rows": sum(r["rows"] for r in rows),
            "max_processed_rps": max((r["processed_rps"] for r in active), default=0),
            "median_trigger_ms": sorted(r["trigger_ms"] for r in active)[len(active) // 2] if active else None,
            "max_state_rows": max((r["state_rows"] for r in rows), default=0),
            "max_state_mem_mb": round(max((r["state_mem_bytes"] for r in rows), default=0) / 2**20, 2),
            "dropped_by_watermark": sum(r["dropped_by_watermark"] for r in rows),
        }

    tables = {}
    for t in ("kpi_events", "kpi_1m", "alerts", "cell_health"):
        f = spark.sql(f"SELECT count(*) n, coalesce(sum(file_size_in_bytes),0) bytes FROM {ns}.{t}.files").collect()[0]
        sn = spark.sql(f"SELECT count(*) n FROM {ns}.{t}.snapshots").collect()[0]
        tables[t] = {"data_files": f["n"], "bytes": f["bytes"], "snapshots": sn["n"]}

    health = {r["state"]: r["n"] for r in spark.sql(f"SELECT state, count(*) n FROM {ns}.cell_health GROUP BY state").collect()}
    alerts_by_type = {r["alert_type"]: r["n"] for r in spark.sql(
        f"SELECT alert_type, count(*) n FROM {ns}.alerts WHERE status='OPEN' GROUP BY alert_type").collect()}

    out = {"run_id": a.run_id, "ns": a.ns, "producer": {k: man[k] for k in (
               "cells", "sim_minutes", "speed", "seed", "wall_seconds", "events_per_wall_second", "incidents", "benign_events")},
           "app_wall_seconds": prog["wall_seconds"], "reconciliation": recon, "detection": quality,
           "static_baseline_same_windows": static_quality,
           "alerts_by_type": alerts_by_type, "latency_seconds": lat, "queries": queries, "tables": tables,
           "cell_health": health}
    Path(a.out).write_text(json.dumps(out, indent=2, default=float))
    print(json.dumps(out, indent=1, default=float))
    spark.stop()


if __name__ == "__main__":
    main()
