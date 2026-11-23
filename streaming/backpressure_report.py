"""Latency of one backpressure run: how far behind the silver query falls when input outruns it.

Structured Streaming has no automatic backpressure for Kafka: `maxOffsetsPerTrigger` caps how much a
micro-batch reads, and anything beyond that waits in Kafka. When the input rate is above
cap / trigger interval, the backlog and therefore the latency grow for as long as the overload lasts.

Reads lake.<ns>.kpi_1m (emitted_at, last_kafka_ts) and the run's progress file, writes
data/runs/<run>/backpressure.json.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--ns", default="bp")
    p.add_argument("--runs-dir", default="/data/runs")
    a = p.parse_args()
    run = Path(a.runs_dir) / a.run_id
    m = json.loads((run / "manifest.json").read_text())
    prog = json.loads((run / f"progress_{a.ns}.json").read_text())
    spark = SparkSession.builder.appName(f"bp-report[{a.run_id}]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    t0 = m["wall_start_epoch"]
    rows = spark.sql(f"""
        SELECT CAST(unix_millis(emitted_at) / 1000.0 - {t0} AS DOUBLE) AS t_s,
               percentile(unix_millis(emitted_at) - unix_millis(last_kafka_ts), 0.5) / 1000.0 AS p50_s,
               CAST(max(unix_millis(emitted_at) - unix_millis(last_kafka_ts)) / 1000.0 AS DOUBLE) AS max_s,
               count(*) AS windows
        FROM lake.{a.ns}.kpi_1m GROUP BY emitted_at ORDER BY emitted_at""").collect()
    allp = spark.sql(f"""SELECT percentile(l, array(0.5, 0.95, 0.99)) p, max(l) mx, count(*) n FROM (
        SELECT CAST((unix_millis(emitted_at) - unix_millis(last_kafka_ts)) / 1000.0 AS DOUBLE) l FROM lake.{a.ns}.kpi_1m)""").first()
    spark.stop()
    cfg = prog["config"]
    batches = prog["queries"]["silver"]
    busy = [b for b in batches if b["rows"]]
    out = {
        "run_id": a.run_id, "speed": m["speed"], "producer_wall_s": m["wall_seconds"],
        "input_rate": m["events_per_wall_second"], "events": m["delivery"]["sent"],
        "max_offsets": cfg["max_offsets"], "trigger_s": cfg["trigger_silver"],
        "cap_rate": round(cfg["max_offsets"] / cfg["trigger_silver"], 1),
        "watermark_wall_s": round(cfg["watermark_min"] * 60 / m["speed"], 2),
        "latency_s": {"p50": allp.p[0], "p95": allp.p[1], "p99": allp.p[2], "max": allp.mx, "windows": allp.n},
        "batches": len(busy),
        "batch_ms_p50": sorted(b["trigger_ms"] for b in busy)[len(busy) // 2] if busy else None,
        "rows_per_batch_max": max((b["rows"] for b in busy), default=0),
        "last_window_emitted_s": rows[-1].t_s if rows else None,
        "timeline": [{"t_s": round(r.t_s, 2), "p50_s": r.p50_s, "max_s": r.max_s, "windows": r.windows} for r in rows],
        "progress": [{k: b[k] for k in ("timestamp", "rows", "trigger_ms")} for b in batches],
    }
    (run / "backpressure.json").write_text(json.dumps(out, indent=1))
    print(json.dumps({k: v for k, v in out.items() if k not in ("timeline", "progress")}))


if __name__ == "__main__":
    main()
