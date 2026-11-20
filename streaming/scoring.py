"""Score alerts against the producer's ground truth (pure pandas, shared by the Spark job and tests)."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

GRACE_MIN = 5   # an alert up to 5 minutes after the incident ends still counts as a detection


def _minutes(ts, sim_start: datetime) -> float:
    return (pd.Timestamp(ts).tz_localize(None) - pd.Timestamp(sim_start).tz_localize(None)).total_seconds() / 60


def score(opens: pd.DataFrame, truth: dict, sim_start: datetime, sim_minutes: int | None = None) -> dict:
    """`opens`: one row per OPEN alert with cell_id, alert_type, window_start (sim time)."""
    incidents = truth["incidents"]
    benign = truth["benign"]
    by_cell: dict[str, list[dict]] = {}
    for i in incidents:
        by_cell.setdefault(i["cell_id"], []).append(i)
    benign_cells: dict[str, list[dict]] = {}
    for b in benign:
        benign_cells.setdefault(b["cell_id"], []).append(b)
    if sim_minutes is not None:   # incidents that end after the run cannot be judged fairly
        incidents = [i for i in incidents if i["start_min"] < sim_minutes - 2]

    detected: dict[str, dict] = {}
    tp = fp = fp_benign = 0
    for r in opens.sort_values("window_start").itertuples(index=False):
        m = _minutes(r.window_start, sim_start)
        hit = None
        for i in by_cell.get(r.cell_id, []):
            if i["start_min"] - 1 <= m < i["end_min"] + GRACE_MIN:
                hit = i
                break
        if hit is None:
            fp += 1
            if any(b["start_min"] - 1 <= m < b["end_min"] + GRACE_MIN for b in benign_cells.get(r.cell_id, [])):
                fp_benign += 1
            continue
        tp += 1
        if hit["incident_id"] not in detected:
            detected[hit["incident_id"]] = {
                "kind": hit["kind"],
                "type_ok": r.alert_type == hit["kind"],
                "delay_min": max(0.0, m + 1 - hit["start_min"]),   # window closes 1 minute after it starts
            }

    rows = []
    for i in incidents:
        d = detected.get(i["incident_id"])
        rows.append({"kind": i["kind"], "detected": d is not None,
                     "type_ok": bool(d and d["type_ok"]), "delay_min": d["delay_min"] if d else None})
    df = pd.DataFrame(rows)
    n_alerts = tp + fp
    precision = tp / n_alerts if n_alerts else 0.0
    recall = df.detected.mean() if len(df) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    per_kind = {}
    for k, g in df.groupby("kind"):
        dd = g[g.detected]
        per_kind[k] = {
            "incidents": int(len(g)), "detected": int(g.detected.sum()),
            "type_correct": int(g.type_ok.sum()),
            "median_delay_min": float(dd.delay_min.median()) if len(dd) else None,
            "max_delay_min": float(dd.delay_min.max()) if len(dd) else None,
        }
    det = df[df.detected]
    return {
        "alerts": int(n_alerts), "true_alerts": int(tp), "false_alerts": int(fp),
        "false_alerts_on_flash_crowds": int(fp_benign),
        "incidents": int(len(df)), "incidents_detected": int(df.detected.sum()) if len(df) else 0,
        "precision": round(precision, 4), "recall": round(float(recall), 4), "f1": round(f1, 4),
        "type_accuracy": round(float(det.type_ok.mean()), 4) if len(det) else 0.0,
        "median_delay_min": float(det.delay_min.median()) if len(det) else None,
        "p90_delay_min": float(det.delay_min.quantile(0.9)) if len(det) else None,
        "per_kind": per_kind,
    }
