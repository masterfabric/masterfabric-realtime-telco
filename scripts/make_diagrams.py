"""Render the architecture, detector and Cloudera-mapping diagrams (Pillow)."""
from __future__ import annotations

from mfviz import (BG, GREEN, GREY, ICE, IMG, LINE, ORANGE, PANEL, PURPLE, RED, WHITE, YELLOW, arrow, box,
                   canvas, font)


def architecture():
    im, d = canvas(1600, 900)
    d.text((60, 40), "MasterFabric Realtime Telco · architecture", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "Kafka feeds two independent Spark queries; the detector reads silver back as an Iceberg stream.",
           font=font(20, 450), fill=GREY)
    box(d, (50, 170, 330, 470), "Producer", ["producer/run.py", "240 cells · 10 s reports", "simulated clock (30x)",
                                              "late · very late", "duplicates", "truth.json"], YELLOW, line_size=17)
    box(d, (400, 170, 690, 470), "Kafka 3.9 KRaft", ["cell.kpi", "  6 partitions", "  key = cell_id", "cell.alerts",
                                                     "  compacted", "kafbat UI"], ICE, line_size=17)
    box(d, (770, 140, 1150, 330), "query: bronze", ["explicit schema · PERMISSIVE", "keeps everything + raw text",
                                                    "trigger 5 s"], ORANGE, line_size=17)
    box(d, (770, 360, 1150, 580), "query: silver", ["watermark 2 min", "dropDuplicatesWithinWatermark",
                                                    "1-minute windows per cell", "trigger 2 s"], ORANGE, line_size=17)
    box(d, (770, 620, 1150, 860), "query: detect", ["Iceberg stream of kpi_1m", "applyInPandasWithState",
                                                    "EWMA z · CUSUM · rules", "foreachBatch: MERGE + Kafka"], RED, line_size=17)
    box(d, (1230, 140, 1560, 560), "Iceberg (REST + S3)", ["rt.kpi_events   bronze", "rt.kpi_1m        silver",
                                                          "rt.alerts         gold", "rt.cell_health  gold",
                                                          "", "RustFS S3 storage", "REST catalog · SQLite",
                                                          "maintenance.py"], PURPLE, line_size=17)
    box(d, (50, 560, 690, 860), "Spark 3.5.5 driver (local[4])", ["RocksDB state store · checkpoints on a volume",
                                                                  "evaluate.py  reconcile · score · latency",
                                                                  "offline.py   same detector, pandas replay",
                                                                  "maintenance.py  compaction · expiry"], GREEN, line_size=17)
    arrow(d, (330, 320), (400, 320), YELLOW, "produce")
    arrow(d, (690, 250), (770, 235), ICE)
    arrow(d, (690, 400), (770, 460), ICE)
    arrow(d, (1150, 235), (1230, 235), PURPLE, "append")
    arrow(d, (1150, 470), (1230, 380), PURPLE, "append", at=0.45)
    arrow(d, (1230, 480), (1150, 680), PURPLE, "stream read", at=0.55)
    arrow(d, (1150, 780), (1300, 560), PURPLE, "MERGE", at=0.4)
    arrow(d, (770, 800), (690, 440), ICE, "cell.alerts", at=0.45)
    im.save(IMG / "architecture.png", optimize=True)


def detector_flow():
    im, d = canvas(1600, 900)
    d.text((60, 40), "One window, one cell: how the detector decides", font=font(36, 800), fill=WHITE)
    d.text((60, 90), "streaming/detector.py · the same code runs inside Spark and in the offline replay",
           font=font(20, 450), fill=GREY)
    box(d, (60, 170, 420, 420), "1-minute window", ["availability_min", "prb_avg · latency_avg", "sinr_avg",
                                                    "rrc_attempts · rrc_success", "from rt.kpi_1m"], ICE, line_size=18)
    box(d, (520, 150, 1060, 330), "Hard rules (no baseline)", ["availability < 50%            → outage",
                                                               "≥20 attempts, >50% fail   → sleeping cell",
                                                               "fire on the first window"], RED, line_size=18)
    box(d, (520, 360, 1060, 560), "Adaptive z-scores (EWMA, α 0.05)", ["PRB ≥ 95% and latency z ≥ 4  → congestion",
                                                                     "SINR z ≤ −4                      → interference",
                                                                     "two consecutive windows"], ORANGE, line_size=18)
    box(d, (520, 590, 1060, 800), "Bernoulli CUSUM on RRC failures", ["LLR of p1 = max(4·p0, p0+1.5%) vs p0",
                                                                     "S = max(0, S + LLR) ≥ 14  → degradation",
                                                                     "counts, not a Gaussian approximation"], YELLOW, line_size=18)
    box(d, (1160, 170, 1550, 470), "Alert state machine", ["most severe condition wins", "OPEN with evidence JSON",
                                                          "CLOSED after 3 healthy windows", "severity: critical/major/minor"],
        PURPLE, line_size=18)
    box(d, (1160, 520, 1550, 800), "Baseline update", ["only on calm windows", "frozen while anomalous", "or an alert is open",
                                                      "warm-up: 30 windows"], GREEN, line_size=18)
    for y in (240, 460, 690):
        arrow(d, (420, 295), (520, y), GREY)
    for y in (240, 460, 690):
        arrow(d, (1060, y), (1160, 320), GREY)
    arrow(d, (1060, 700), (1160, 660), GREEN)
    d.text((60, 850), "Tuned offline on seed 7 (dev); scored live on seed 2026 (held out).", font=font(20, 500), fill=GREY)
    im.save(IMG / "detector_flow.png", optimize=True)


def cloudera_mapping():
    im, d = canvas(1600, 760)
    d.text((60, 40), "How the lab maps to a Cloudera CDP streaming estate", font=font(36, 800), fill=WHITE)
    rows = [("Event backbone", "Kafka 3.9 KRaft, 1 broker", "CDP Streams Messaging (Kafka, RF 3, Kerberos/TLS)"),
            ("Ingest from OSS", "producer/run.py (simulated)", "NiFi / Cloudera DataFlow from mediation & PM files"),
            ("Topic tooling", "kafbat/kafka-ui", "Streams Messaging Manager · Schema Registry"),
            ("Stream processing", "Spark Structured Streaming 3.5", "CDE / Data Hub Spark, or Cloudera Streaming Analytics (Flink)"),
            ("State", "RocksDB state store, local volume", "RocksDB on executors, checkpoints on HDFS / Ozone"),
            ("Tables", "Iceberg 1.8 · REST catalog · RustFS", "Iceberg on Ozone/HDFS · Hive Metastore · SDX"),
            ("Alerts out", "cell.alerts (compacted) + rt.alerts", "Kafka → NOC / ticketing (ServiceNow, Remedy)"),
            ("Security", "loopback-only, PLAINTEXT, lab creds", "Ranger policies, Kerberos, TLS, Atlas lineage")]
    y = 120
    d.text((80, y), "Concern", font=font(20, 700), fill=GREY)
    d.text((420, y), "This lab", font=font(20, 700), fill=ICE)
    d.text((900, y), "Cloudera CDP", font=font(20, 700), fill=ORANGE)
    y += 44
    for i, (a, b, c) in enumerate(rows):
        if i % 2 == 0:
            d.rounded_rectangle((60, y - 8, 1540, y + 62), radius=12, fill=PANEL)
        d.text((80, y + 12), a, font=font(21, 700), fill=WHITE)
        d.text((420, y + 12), b, font=font(20, 450), fill=WHITE)
        d.text((900, y + 12), c, font=font(20, 450), fill=WHITE)
        y += 72
    im.save(IMG / "cloudera_mapping.png", optimize=True)


if __name__ == "__main__":
    architecture()
    detector_flow()
    cloudera_mapping()
    print("diagrams written to", IMG)
