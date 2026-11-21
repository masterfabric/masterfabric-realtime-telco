"""Charts from the measured results in docs/results (matplotlib, dark theme)."""
from __future__ import annotations

import json
import sys

import numpy as np

from mfviz import HEX, IMG, RESULTS, mpl_setup

plt = mpl_setup()
RUN = sys.argv[1] if len(sys.argv) > 1 else "live"
C = {"outage": HEX["RED"], "sleeping_cell": HEX["PURPLE"], "congestion": HEX["ORANGE"],
     "interference": HEX["YELLOW"], "degradation": HEX["ICE"], "flash_crowd": HEX["GREEN"]}
KPI = {"outage": ("availability_min", "availability %"), "sleeping_cell": ("rrc_sr", "RRC success ratio"),
       "congestion": ("latency_avg", "latency ms"), "interference": ("sinr_avg", "UL SINR dB"),
       "degradation": ("rrc_sr", "RRC success ratio"), "flash_crowd": ("prb_avg", "PRB utilisation %")}


def traces():
    tr = json.loads((RESULTS / f"live_{RUN}_traces.json").read_text())
    order = ["outage", "sleeping_cell", "congestion", "interference", "degradation", "flash_crowd"]
    tr = sorted(tr, key=lambda t: order.index(t["incident"]["kind"]))
    fig, axes = plt.subplots(2, 3, figsize=(15, 7.2))
    for ax, t in zip(axes.flat, tr):
        i = t["incident"]
        col, label = KPI[i["kind"]]
        ax.axvspan(i["start_min"], i["end_min"], color=C[i["kind"]], alpha=0.18, lw=0)
        ax.plot(t["minute"], t[col], color=C[i["kind"]], lw=1.8)
        for al in t["alerts"]:
            if t["minute"] and t["minute"][0] <= al["minute"] <= t["minute"][-1]:
                ax.axvline(al["minute"] + 1, color=HEX["RED"] if al["status"] == "OPEN" else HEX["GREEN"],
                           ls="--", lw=1.4)
        benign = i["kind"] == "flash_crowd"
        ax.set_title(f"{i['kind'].replace('_', ' ')}{' (benign)' if benign else ''} · {i['cell_id']}", loc="left")
        ax.set_ylabel(label)
        ax.set_xlabel("simulated minute")
    fig.suptitle("Live run: KPI per 1-minute window around one incident of each kind. Shaded: ground truth. "
                 "Dashed: alert OPEN (red) / CLOSED (green)", x=0.01, ha="left", fontsize=12, color=HEX["GREY"])
    fig.tight_layout()
    fig.savefig(IMG / "detection_traces.png")
    plt.close(fig)


def comparison():
    r = json.loads((RESULTS / f"live_{RUN}.json").read_text())
    a, s = r["detection"], r["static_baseline_same_windows"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.6), gridspec_kw={"width_ratios": [3, 2]})
    m = ["precision", "recall", "f1"]
    x = np.arange(len(m))
    b1 = ax1.bar(x - 0.2, [a[k] for k in m], 0.38, color=HEX["GREEN"], label="adaptive detector (streaming)")
    b2 = ax1.bar(x + 0.2, [s[k] for k in m], 0.38, color=HEX["GREY"], label="static thresholds (same windows)")
    for bars in (b1, b2):
        for b in bars:
            ax1.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.02, f"{b.get_height():.2f}", ha="center")
    ax1.set_xticks(x, ["precision", "recall", "F1"])
    ax1.set_ylim(0, 1.15)
    ax1.legend(loc="upper left", ncols=2)
    ax1.set_title(f"Held-out seed {r['producer']['seed']}: {a['incidents']} incidents, {r['producer']['cells']} cells, "
                  f"{r['producer']['sim_minutes'] // 60} h", loc="left")
    ax2.barh(["adaptive", "static"], [a["false_alerts"], s["false_alerts"]], color=[HEX["GREEN"], HEX["GREY"]])
    for yv, v in enumerate([a["false_alerts"], s["false_alerts"]]):
        ax2.text(v, yv, f"  {v}", va="center")
    ax2.set_title("false alerts", loc="left")
    ax2.set_xlim(0, max(s["false_alerts"], 1) * 1.2)
    fig.tight_layout()
    fig.savefig(IMG / "adaptive_vs_static.png")
    plt.close(fig)


def latency():
    L = json.loads((RESULTS / f"live_{RUN}_latency.json").read_text())
    fig, ax = plt.subplots(figsize=(11, 4.2))
    for key, col, lab in (("silver", HEX["ICE"], "last report → silver window committed"),
                          ("alert", HEX["RED"], "last report → alert in rt.alerts + cell.alerts")):
        v = np.sort(np.array(L[key]))
        if len(v):
            ax.plot(v, np.arange(1, len(v) + 1) / len(v), color=col, lw=2.2,
                    label=f"{lab}  (p50 {np.percentile(v, 50):.1f} s · p95 {np.percentile(v, 95):.1f} s, n={len(v)})")
    ax.set_xlabel("wall-clock seconds (Kafka timestamp of the window's last report → commit)")
    ax.set_ylabel("share of windows / alerts")
    ax.legend(loc="lower right")
    ax.set_title("End-to-end latency, live run (cumulative distribution)", loc="left")
    fig.tight_layout()
    fig.savefig(IMG / "latency_cdf.png")
    plt.close(fig)


def throughput():
    pr = json.loads(open(f"data/runs/{RUN}/progress_rt.json").read())["queries"]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(14, 4.4), gridspec_kw={"width_ratios": [3, 2]})
    t0 = min(np.datetime64(r["timestamp"][:23]) for q in pr.values() for r in q)
    trig = {"bronze": 5, "silver": 2, "detect": 2}
    for q, col in (("bronze", HEX["ORANGE"]), ("silver", HEX["ICE"]), ("detect", HEX["RED"])):
        rows = [r for r in pr[q] if r["rows"] > 0]
        sec = [(np.datetime64(r["timestamp"][:23]) - t0) / np.timedelta64(1, "s") for r in rows]
        ax.plot(sec, [r["trigger_ms"] / 1000 for r in rows], color=col, lw=1.2,
                label=f"{q} (trigger {trig[q]} s, median {np.median([r['trigger_ms'] for r in rows]) / 1000:.2f} s)")
    ax.axhline(2, color=HEX["GREY"], ls=":", lw=1)
    ax.set_ylabel("micro-batch duration (s)")
    ax.set_xlabel("seconds since the queries started")
    ax.set_title("Every batch finished inside its trigger interval: no backlog at ~724 events/s", loc="left")
    ax.legend(loc="upper right", fontsize=9)
    rows = pr["silver"]
    sec = [(np.datetime64(r["timestamp"][:23]) - t0) / np.timedelta64(1, "s") for r in rows]
    ax2.plot(sec, [r["state_rows"] for r in rows], color=HEX["ICE"], lw=1.6, label="state rows (windows + dedupe keys)")
    ax3 = ax2.twinx()
    ax3.plot(sec, [r["state_mem_bytes"] / 2**20 for r in rows], color=HEX["PURPLE"], lw=1.6, label="RocksDB memory (MiB)")
    ax3.grid(False)
    ax2.set_title(f"silver: state rows bounded by the watermark; RocksDB memory grew to {max(r['state_mem_bytes'] for r in rows) / 2**20:.0f} MiB", loc="left", fontsize=11)
    ax2.set_xlabel("seconds")
    l1, n1 = ax2.get_legend_handles_labels()
    l2, n2 = ax3.get_legend_handles_labels()
    ax2.legend(l1 + l2, n1 + n2, loc="lower right", fontsize=9)
    fig.tight_layout()
    fig.savefig(IMG / "stream_batches.png")
    plt.close(fig)


def cusum_sweep():
    r = json.loads((RESULTS / "offline_dev_seed7.json").read_text())["cusum_sweep"]
    fig, ax = plt.subplots(figsize=(8, 4))
    h = [x["cusum_h"] for x in r]
    ax.plot(h, [x["false_alerts"] for x in r], "o-", color=HEX["RED"], lw=2, label="false alerts (all types)")
    ax2 = ax.twinx()
    ax2.plot(h, [x["degradation_median_delay_min"] for x in r], "s--", color=HEX["ICE"], lw=2,
             label="median degradation delay (min)")
    ax2.grid(False)
    ax.axvline(14, color=HEX["GREEN"], lw=1.2, ls=":")
    ax.text(14.1, max(x["false_alerts"] for x in r) * 0.9, "chosen h = 14", color=HEX["GREEN"])
    ax.set_xlabel("CUSUM decision interval h (nats)")
    ax.set_ylabel("false alerts")
    ax2.set_ylabel("minutes")
    l1, n1 = ax.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax.legend(l1 + l2, n1 + n2, loc="upper center")
    ax.set_title("Tuning on the development seed (offline replay, recall = 1.0 at every h)", loc="left")
    fig.tight_layout()
    fig.savefig(IMG / "cusum_sweep.png")
    plt.close(fig)


def bench():
    p = RESULTS / "bench_throughput.json"
    if not p.exists():
        return
    r = json.loads(p.read_text())["runs"]
    fig, ax = plt.subplots(figsize=(11, 4.4))
    labels = [x["label"] for x in r]
    vals = [x["rows_per_s"] for x in r]
    bars = ax.barh(labels, vals, color=[HEX["ORANGE"] if "bronze" in x["label"] else HEX["ICE"] for x in r])
    for b, v in zip(bars, vals):
        ax.text(v, b.get_y() + b.get_height() / 2, f"  {v:,.0f}", va="center")
    ax.invert_yaxis()
    ax.set_xlabel("events per second, draining a pre-filled topic (availableNow)")
    ax.set_xlim(0, max(vals) * 1.18)
    ax.set_title("Throughput on the laptop: Spark local[4], 6 Kafka partitions", loc="left")
    fig.tight_layout()
    fig.savefig(IMG / "bench_throughput.png")
    plt.close(fig)


if __name__ == "__main__":
    cusum_sweep()
    if (RESULTS / f"live_{RUN}.json").exists():
        traces()
        comparison()
        latency()
        throughput()
    bench()
    print("charts written to", IMG)
