"""Offline replay: run the exact streaming detector over a producer file dump with pandas.

Used to tune thresholds on a development seed before a live run, and to compare against the
static-threshold baseline. The live pipeline is evaluated on a different seed.

    python -m streaming.offline --events /data/runs/dev/events.jsonl --sweep-cusum 8,10,12,14,16
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from streaming import detector
from streaming.scoring import score


def windows_from_events(path: Path) -> pd.DataFrame:
    df = pd.read_json(path, lines=True).drop_duplicates("event_id")
    df["window_start"] = pd.to_datetime(df.event_ts).dt.floor("1min")
    return (df.groupby(["cell_id", "window_start"])
              .agg(prb_avg=("prb_util_pct", "mean"), latency_avg=("latency_ms", "mean"),
                   sinr_avg=("ul_sinr_db", "mean"), rrc_attempts=("rrc_attempts", "sum"),
                   rrc_success=("rrc_success", "sum"), availability_min=("availability_pct", "min"))
              .reset_index().sort_values(["cell_id", "window_start"]))


def replay(w: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    adaptive, static = [], []
    for cell, g in w.groupby("cell_id"):
        st, prev = detector.CellState(), None
        for r in g.to_dict("records"):
            r["window_start"] = str(r["window_start"])
            adaptive += [e for e in detector.step(st, r) if e["status"] == "OPEN"]
            k = detector.static_rules(r)
            if k and k != prev:
                static.append({"cell_id": cell, "alert_type": k, "window_start": r["window_start"]})
            prev = k
    return pd.DataFrame(adaptive), pd.DataFrame(static)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--events", required=True)
    p.add_argument("--sweep-cusum", default="")
    p.add_argument("--out", default=None)
    a = p.parse_args()
    run_dir = Path(a.events).parent
    truth = json.loads((run_dir / "truth.json").read_text())
    man = json.loads((run_dir / "manifest.json").read_text())
    sim_start = datetime.fromisoformat(man["sim_start"])
    w = windows_from_events(Path(a.events))
    out: dict = {"run_id": man["run_id"], "seed": man["seed"], "windows": int(len(w))}
    adaptive, static = replay(w)
    out["adaptive"] = score(adaptive, truth, sim_start, man["sim_minutes"])
    out["static"] = score(static, truth, sim_start, man["sim_minutes"])
    if a.sweep_cusum:
        default_h = detector.CUSUM_H
        out["cusum_sweep"] = []
        for h in [float(x) for x in a.sweep_cusum.split(",")]:
            detector.CUSUM_H = h
            s = score(replay(w)[0], truth, sim_start, man["sim_minutes"])
            out["cusum_sweep"].append({"cusum_h": h, "false_alerts": s["false_alerts"], "recall": s["recall"],
                                       "degradation_median_delay_min": s["per_kind"]["degradation"]["median_delay_min"]})
        detector.CUSUM_H = default_h
    text = json.dumps(out, indent=2)
    if a.out:
        Path(a.out).write_text(text)
    print(text)


if __name__ == "__main__":
    main()
