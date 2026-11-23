"""How long to wait for late data: sweep the silver watermark over one recorded delivery.

The producer's file dump (with --arrivals) is cut into micro-batches by simulated arrival time and
replayed through `silver_frame` once per watermark delay. For each delay we measure what the
watermark costs and what it buys:

- reports lost: distinct reports minus the reports counted in emitted windows,
- complete windows: windows whose report count equals the distinct reports delivered for them,
- emit delay: simulated seconds from a window's end to the arrival batch that emitted it.

    python -m streaming.watermark_sweep --run /data/runs/wm-seed2026 --out docs/results/watermark_sweep.json
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from streaming.replay import Replay, event, local_spark

SIM_START = datetime(2026, 11, 16, 5, 0, tzinfo=timezone.utc)


def sim_s(ts: str) -> float:
    return (datetime.fromisoformat(ts.replace("Z", "+00:00")) - SIM_START).total_seconds()


def load(run: Path, batch_s: int):
    events = [json.loads(line) for line in open(run / "events.jsonl")]
    arrivals = [float(x) for x in open(run / "arrivals.txt")]
    assert len(events) == len(arrivals)
    batches: dict[int, list[dict]] = defaultdict(list)
    lags = []
    for e, at in zip(events, arrivals):
        e["kafka_ts"] = e["event_ts"]
        batches[int(at // batch_s)].append(e)
        lags.append(at - sim_s(e["event_ts"]))
    expected: dict[tuple, set] = defaultdict(set)      # (cell, window start s) -> distinct event_ids
    for e in events:
        expected[(e["cell_id"], int(sim_s(e["event_ts"]) // 60) * 60)].add(e["event_id"])
    return events, batches, np.array(lags), {k: len(v) for k, v in expected.items()}


def run_one(spark, batches, expected, watermark: str, batch_s: int) -> dict:
    rp = Replay(spark, watermark)
    t0 = time.perf_counter()
    for k in sorted(batches):
        rp.push(batches[k], tag=(k + 1) * batch_s)
    rp.push([event("zz-flush", "2026-11-17T05:00:00Z")], tag="flush")  # closes every remaining window
    seconds = time.perf_counter() - t0
    rows = [r for r in rp.rows if r["cell_id"] != "zz-flush"]
    got = {(r["cell_id"], int((r["window_start"].replace(tzinfo=timezone.utc) - SIM_START).total_seconds())): r
           for r in rows}
    delays = [r["emitted_with"] - ((ws + 60)) for (c, ws), r in got.items() if r["emitted_with"] != "flush"]
    counted = sum(r["reports"] for r in rows)
    complete = sum(1 for k, n in expected.items() if k in got and got[k]["reports"] == n)
    out = {"watermark": watermark, "windows_expected": len(expected), "windows_emitted": len(got),
           "windows_complete": complete, "windows_missing": len(expected) - len(got),
           "reports_distinct": sum(expected.values()), "reports_counted": counted,
           "reports_lost": sum(expected.values()) - counted, "dropped_by_watermark_rows": rp.dropped,
           "emitted_by_flush": sum(1 for r in rows if r["emitted_with"] == "flush"),
           "emit_delay_s": {"p50": float(np.percentile(delays, 50)), "p95": float(np.percentile(delays, 95)),
                            "max": float(np.max(delays))},
           "spark_batches": len(rp.progress), "replay_seconds": round(seconds, 1)}
    rp.stop()
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", required=True)
    p.add_argument("--batch-s", type=int, default=30, help="simulated seconds of arrivals per micro-batch")
    p.add_argument("--watermarks", default="10 seconds,30 seconds,1 minute,2 minutes,5 minutes,11 minutes")
    p.add_argument("--out", required=True)
    a = p.parse_args()
    run = Path(a.run)
    events, batches, lags, expected = load(run, a.batch_s)
    manifest = json.loads((run / "manifest.json").read_text())
    spark = local_spark("watermark-sweep")
    spark.sparkContext.setLogLevel("ERROR")
    results = []
    for w in a.watermarks.split(","):
        r = run_one(spark, batches, expected, w.strip(), a.batch_s)
        print(json.dumps(r), flush=True)
        results.append(r)
    spark.stop()
    hist = Counter(np.digitize(lags, [2, 10, 30, 60, 120, 300, 600]).tolist())
    edges = ["<2 s", "2-10 s", "10-30 s", "30-60 s", "60-120 s", "120-300 s", "300-600 s", ">600 s"]
    Path(a.out).write_text(json.dumps({
        "run_id": manifest["run_id"], "cells": manifest["cells"], "sim_minutes": manifest["sim_minutes"],
        "seed": manifest["seed"], "delivery": manifest["delivery"], "rates": manifest["rates"],
        "events_delivered": len(events), "batch_s": a.batch_s,
        "arrival_lag_s": {"p50": float(np.percentile(lags, 50)), "p99": float(np.percentile(lags, 99)),
                          "p999": float(np.percentile(lags, 99.9)), "max": float(lags.max()),
                          "histogram": {edges[i]: hist.get(i, 0) for i in range(len(edges))}},
        "results": results}, indent=1))


if __name__ == "__main__":
    main()
