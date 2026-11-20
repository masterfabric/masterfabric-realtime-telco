"""Table maintenance after (or between) streaming runs: compact small files, expire snapshots.

Streaming sinks commit every few seconds, so a few hours of data becomes hundreds of small files and
snapshots. Readers pay for every file they open. This job measures the before/after state.

    spark-submit streaming/maintenance.py --ns rt --out docs/results/maintenance_live.json
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession


def stats(spark: SparkSession, t: str) -> dict:
    f = spark.sql(f"SELECT count(*) n, coalesce(sum(file_size_in_bytes),0) b, coalesce(avg(file_size_in_bytes),0) a FROM {t}.files").collect()[0]
    s = spark.sql(f"SELECT count(*) n FROM {t}.snapshots").collect()[0]
    m = spark.sql(f"SELECT count(*) n FROM {t}.manifests").collect()[0]
    return {"data_files": f["n"], "bytes": f["b"], "avg_file_kib": round(f["a"] / 1024, 1),
            "snapshots": s["n"], "manifests": m["n"]}


def timed_query(spark: SparkSession, sql: str, repeat: int = 5) -> float:
    spark.sql(sql).collect()                     # warm-up
    t = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        t.append(time.perf_counter() - t0)
    return round(sorted(t)[len(t) // 2] * 1000, 1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--ns", default="rt")
    p.add_argument("--out", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName("mf-realtime-maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    probe = f"SELECT cell_id, avg(prb_avg) FROM lake.{a.ns}.kpi_1m GROUP BY cell_id"
    out = {"ns": a.ns, "tables": {}, "probe_query": probe}
    out["probe_ms_before"] = timed_query(spark, probe)
    for t in ("kpi_events", "kpi_1m", "alerts", "cell_health"):
        name = f"lake.{a.ns}.{t}"
        before = stats(spark, name)
        t0 = time.perf_counter()
        r = spark.sql(f"CALL lake.system.rewrite_data_files(table => '{a.ns}.{t}', "
                      f"options => map('min-input-files','2','target-file-size-bytes','134217728'))").collect()[0]
        spark.sql(f"CALL lake.system.rewrite_manifests('{a.ns}.{t}')").collect()
        e = spark.sql(f"CALL lake.system.expire_snapshots(table => '{a.ns}.{t}', "
                      f"older_than => current_timestamp(), retain_last => 1)").collect()[0]
        out["tables"][t] = {"before": before, "after": stats(spark, name),
                            "rewritten_files": r["rewritten_data_files_count"], "added_files": r["added_data_files_count"],
                            "expired_data_files": e["deleted_data_files_count"],
                            "seconds": round(time.perf_counter() - t0, 2)}
    out["probe_ms_after"] = timed_query(spark, probe)
    Path(a.out).write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=1))
    spark.stop()


if __name__ == "__main__":
    main()
