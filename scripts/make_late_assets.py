"""Charts for the late-data and backpressure addition.

    .venv/bin/python scripts/make_late_assets.py
Reads docs/results/watermark_sweep.json and docs/results/backpressure.json.
"""
from __future__ import annotations

import json

from mfviz import HEX, IMG, RESULTS, mpl_setup


def load(name: str) -> dict:
    return json.loads((RESULTS / f"{name}.json").read_text())


def watermark_tradeoff() -> None:
    plt = mpl_setup()
    d = load("watermark_sweep")
    r = d["results"]
    labels = [x["watermark"].replace(" seconds", " s").replace(" minutes", " min").replace(" minute", " min") for x in r]
    fig, ax = plt.subplots(1, 3, figsize=(16, 5.2))
    h = d["arrival_lag_s"]["histogram"]
    ax[0].bar(list(h), list(h.values()), color=HEX["ICE"])
    ax[0].set_yscale("log")
    for i, v in enumerate(h.values()):
        if v:
            ax[0].text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=9)
    ax[0].set_title(f"Arrival lag of {d['events_delivered']:,} reports (seed {d['seed']})")
    ax[0].set_ylabel("reports (log scale)")
    ax[0].tick_params(axis="x", rotation=35)
    lost = [x["reports_lost"] for x in r]
    inc = [100 * (1 - x["windows_complete"] / x["windows_expected"]) for x in r]
    b = ax[1].bar(labels, lost, color=[HEX["RED"] if v > 500 else HEX["ORANGE"] if v else HEX["GREEN"] for v in lost])
    for bb, v, p in zip(b, lost, inc):
        ax[1].text(bb.get_x() + bb.get_width() / 2, v, f"{v:,}\n{p:.2f}%", ha="center", va="bottom", fontsize=9)
    ax[1].set_ylim(0, max(lost) * 1.35)
    ax[1].set_title("Reports lost (and % windows incomplete)")
    ax[1].set_xlabel("watermark delay")
    delay = [x["emit_delay_s"]["p50"] / 60 for x in r]
    ax[2].plot(labels, delay, "o-", color=HEX["PURPLE"])
    for i, v in enumerate(delay):
        ax[2].text(i, v, f" {v:.1f} min", va="bottom", fontsize=9)
    ax[2].set_title(f"Window end → emitted ({d['batch_s']} s batches)")
    ax[2].set_ylabel("simulated minutes (= real minutes at 1x)")
    ax[2].set_xlabel("watermark delay")
    fig.suptitle("Watermark delay: completeness against freshness, same delivery replayed six times",
                 x=0.01, ha="left", fontsize=13)
    fig.tight_layout()
    fig.savefig(IMG / "watermark_tradeoff.png")
    plt.close(fig)


def backpressure() -> None:
    plt = mpl_setup()
    runs = load("backpressure")["runs"]
    colors = [HEX["GREEN"], HEX["ICE"], HEX["YELLOW"], HEX["RED"], HEX["PURPLE"]]
    fig, ax = plt.subplots(1, 2, figsize=(16, 5.4))
    for run, c in zip(runs, colors):
        cap = run["max_offsets"] < 10**5
        name = (f"{run['input_rate']:,.0f} ev/s = {run['input_rate'] / run['cap_rate']:.1f}x cap" if cap
                else f"{run['input_rate']:,.0f} ev/s, no cap")
        tl = run["timeline"]
        ax[0].plot([t["t_s"] for t in tl], [t["p50_s"] for t in tl], color=c, lw=1.6, label=name,
                   ls="-" if cap else "--")
        ax[0].axvline(run["producer_wall_s"], color=c, lw=0.6, ls=":")
    ax[0].set_xlabel("wall seconds since the producer started (dotted: producer finished)")
    ax[0].set_ylabel("latency, last report → silver row (s)")
    ax[0].set_title(f"Silver latency over time, cap {runs[0]['max_offsets']:,} records per {runs[0]['trigger_s']} s trigger")
    ax[0].legend(fontsize=9, loc="upper left")
    names = [f"{r['input_rate'] / 1000:.2f}k/s" + ("" if r["max_offsets"] < 10**5 else "\nno cap") for r in runs]
    x = range(len(runs))
    for off, q, a in ((-0.2, "p50", 1.0), (0.2, "max", 0.55)):
        ax[1].bar([i + off for i in x], [r["latency_s"][q] for r in runs], width=0.38, color=colors[:len(runs)],
                  alpha=a, label=q)
    for i, r in enumerate(runs):
        ax[1].text(i + 0.2, r["latency_s"]["max"], f"{r['latency_s']['max']:.0f} s", ha="center", va="bottom", fontsize=9)
    ax[1].set_xticks(list(x), names)
    ax[1].set_ylabel("seconds")
    ax[1].set_title("Latency p50 (solid) and max (light) per input rate")
    fig.suptitle("Backpressure: Spark reads at most maxOffsetsPerTrigger, the rest waits in Kafka",
                 x=0.01, ha="left", fontsize=13)
    fig.tight_layout()
    fig.savefig(IMG / "backpressure.png")
    plt.close(fig)


if __name__ == "__main__":
    import sys

    watermark_tradeoff()
    if "--no-backpressure" not in sys.argv:
        backpressure()
    print("ok")
