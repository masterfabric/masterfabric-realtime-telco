"""Diagram and charts for the schema-registry / Flink vs Spark / exactly-once addition.

    .venv/bin/python scripts/make_engine_assets.py
Reads docs/results/{schema_registry,engine_drain,engine_live,exactly_once_flink,exactly_once_spark}.json.
"""
from __future__ import annotations

import json

from mfviz import (GREEN, GREY, HEX, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, WHITE, YELLOW, arrow, box, canvas,
                   font, mpl_setup)


def load(name: str) -> dict:
    return json.loads((RESULTS / f"{name}.json").read_text())


def flow() -> None:
    reg, drain = load("schema_registry"), load("engine_drain")
    im, d = canvas(1600, 900)
    d.text((60, 40), "One Avro topic, two engines, one contract", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Schemas live in the registry; Spark and Flink run the same 1-minute silver aggregation "
                     "and are compared window by window.", font=font(20, 450), fill=GREY)
    b = reg["sample_record_bytes"]
    box(d, (50, 170, 390, 470), "Producer --format avro", ["Confluent wire format", "0x00 + 4-byte schema id + body",
                                                          f"sample record {b['avro_wire']} B vs JSON {b['json']} B",
                                                          "v1 → v2 mid-run", "(adds optional energy_kwh)"], YELLOW, line_size=17)
    box(d, (50, 560, 390, 860), "Apicurio Registry 3.3", ["ccompat API /apis/ccompat/v7", "subject cell.kpi.avro-value",
                                                         "rule: BACKWARD", "v2 accepted · v3 rejected 409",
                                                         "storage: Kafka journal"], PURPLE, line_size=17)
    box(d, (470, 300, 770, 560), "Kafka cell.kpi.avro", ["6 partitions", "v1 + v2 records mixed",
                                                         f"drain test {drain['input']['records']:,} records"], ICE, line_size=17)
    box(d, (860, 140, 1230, 400), "Spark 3.5 Structured Streaming", ["from_avro per schema id (CASE)",
                                                                     "reader schema = latest", "watermark 2 min · dedupe",
                                                                     "1-min windows · Kafka sink (ALO)"], ORANGE, line_size=17)
    box(d, (860, 470, 1230, 760), "Flink 2.2 SQL", ["avro-confluent format", "writer schema fetched by id",
                                                    "window dedupe + TUMBLE", "Kafka sink: at-least-once",
                                                    "or exactly-once (2PC)"], GREEN, line_size=17)
    box(d, (1310, 230, 1560, 690), "Compare", ["kpi_1m.spark", "kpi_1m.flink", "kpi_1m.flink_eo", "",
                                               "same windows?", "throughput", "latency", "kill -9 →", "duplicates?"],
        RED, line_size=17)
    arrow(d, (390, 330), (470, 400), YELLOW, "produce")
    arrow(d, (220, 470), (220, 560), PURPLE, "register")
    arrow(d, (770, 400), (860, 280), ICE)
    arrow(d, (770, 460), (860, 600), ICE)
    arrow(d, (860, 700), (390, 760), PURPLE, "schema by id", at=0.5)
    arrow(d, (1230, 270), (1310, 330), ORANGE)
    arrow(d, (1230, 610), (1310, 560), GREEN)
    im.save(IMG / "engines_flow.png", optimize=True)


def comparison() -> None:
    plt = mpl_setup()
    drain, live = load("engine_drain"), load("engine_live")
    fig, ax = plt.subplots(1, 3, figsize=(16, 5.4))
    s = drain["summary"]
    names, vals = ["Spark", "Flink"], [s["spark"]["records_per_s"] / 1000, s["flink"]["records_per_s"] / 1000]
    bars = ax[0].bar(names, vals, color=[HEX["ORANGE"], HEX["GREEN"]], width=0.55)
    for bb, v, e in zip(bars, vals, ("spark", "flink")):
        ax[0].text(bb.get_x() + bb.get_width() / 2, v, f"{v:.0f}k/s\n{s[e]['processing_s_median']} s",
                   ha="center", va="bottom", fontsize=11)
    ax[0].set_title(f"Drain {drain['input']['records'] / 1e6:.2f}M Avro records (median of 3)")
    ax[0].set_ylabel("thousand records / s")
    ax[0].set_ylim(0, max(vals) * 1.3)
    L = live["latency"]
    labels = [("spark", "Spark\n(1 s trigger)", HEX["ORANGE"]), ("flink_at_least_once", "Flink\nat-least-once", HEX["GREEN"]),
              ("flink_exactly_once", "Flink\nexactly-once", HEX["PURPLE"])]
    x = range(len(labels))
    for off, q, alpha in ((-0.25, "p50_s", 1.0), (0, "p95_s", 0.7), (0.25, "p99_s", 0.45)):
        ax[1].bar([i + off for i in x], [L[k][q] for k, _, _ in labels], width=0.24,
                  color=[c for _, _, c in labels], alpha=alpha, label=q.replace("_s", ""))
    ax[1].axhline(live["watermark_floor_s"], color=HEX["GREY"], ls="--", lw=1)
    ax[1].text(-0.42, live["watermark_floor_s"] - 0.45, "2-min watermark = 2 s at 60x", ha="left", fontsize=9, color=HEX["GREY"])
    ax[1].set_xticks(list(x), [lbl for _, lbl, _ in labels])
    ax[1].set_ylabel("seconds after the window's last on-time input")
    ax[1].set_title(f"Live latency, {live['input']['events_per_s']:,.0f} events/s (p50 · p95 · p99)")
    ax[1].legend(loc="upper left", fontsize=9)
    st = live["stats_during_produce"]
    cpu = [st["mf-rt-spark"]["cpu_avg_pct"], st["mf-rt-flink-tm"]["cpu_avg_pct"]]
    mem = [st["mf-rt-spark"]["mem_max_mib"] / 1024, st["mf-rt-flink-tm"]["mem_max_mib"] / 1024]
    bars = ax[2].bar(["Spark container", "Flink TaskManager\n(2 jobs)"], cpu, color=[HEX["ORANGE"], HEX["GREEN"]], width=0.55)
    for bb, c, m in zip(bars, cpu, mem):
        ax[2].text(bb.get_x() + bb.get_width() / 2, c, f"{c:.0f}% CPU\npeak {m:.1f} GiB", ha="center", va="bottom", fontsize=11)
    ax[2].set_ylim(0, max(cpu) * 1.35)
    ax[2].set_ylabel("average CPU % (100 = one core)")
    ax[2].set_title("Resources during the live run (docker stats)")
    fig.tight_layout()
    fig.savefig(IMG / "engine_comparison.png")


def exactly_once() -> None:
    plt = mpl_setup()
    f, s = load("exactly_once_flink"), load("exactly_once_spark")
    t = f["topics"]
    rows = [("Flink exactly-once\nread_committed", t["eo.flink.exactly_once"]["read_committed"]["duplicate_rows"], HEX["GREEN"]),
            ("Flink exactly-once\nread_uncommitted", t["eo.flink.exactly_once"]["read_uncommitted"]["duplicate_rows"], HEX["YELLOW"]),
            ("Flink at-least-once", t["eo.flink.at_least_once"]["read_committed"]["duplicate_rows"], HEX["RED"]),
            ("Spark → Iceberg\n(3 driver kills)", s["iceberg_sink"]["rows"] - s["iceberg_sink"]["distinct_offsets"], HEX["GREEN"]),
            ("Spark → Kafka\n(3 driver kills)", s["kafka_sink"]["duplicate_rows"], HEX["ORANGE"])]
    fig, ax = plt.subplots(figsize=(13, 5.2))
    bars = ax.bar([r[0] for r in rows], [r[1] for r in rows], color=[r[2] for r in rows], width=0.6)
    for bb, r in zip(bars, rows):
        ax.text(bb.get_x() + bb.get_width() / 2, r[1], f"{r[1]:,}", ha="center", va="bottom", fontsize=12)
    k = f["kills"]
    ax.set_title(f"Duplicate output rows after kill -9 (Flink: {len(k)} TaskManager kills, "
                 f"checkpoint {f['setup']['checkpoint']}; {t['eo.flink.exactly_once']['read_committed']['distinct_windows']:,} windows)")
    ax.set_ylabel("duplicate rows")
    ax.set_ylim(0, max(r[1] for r in rows) * 1.2 + 1)
    fig.tight_layout()
    fig.savefig(IMG / "exactly_once.png")


if __name__ == "__main__":
    flow()
    comparison()
    exactly_once()
    print("ok")
