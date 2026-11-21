"""Compose docs/hero.png (1600x900) from real screenshots and measured numbers.

Every number on the hero is read from docs/results/*.json at render time.
"""

from __future__ import annotations

import json

from PIL import Image, ImageDraw, ImageFilter

from mfviz import BG, BG2, DOCS, GREEN, GREY, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, SHOTS, WHITE, YELLOW, font

W, H = 1600, 900


def load(n):
    return json.loads((RESULTS / f"{n}.json").read_text())


def background():
    im = Image.new("RGB", (W, H), BG)
    px = im.load()
    for y in range(H):
        for x in range(W):
            t = min(1.0, (x / W) * 0.75 + (y / H) * 0.35)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im


def card(base, path, box, label, color, crop=None):
    x0, y0, x1, y1 = box
    shot = Image.open(path).convert("RGB")
    if crop:
        shot = shot.crop(crop)
    shot = shot.resize((x1 - x0, round(shot.height * (x1 - x0) / shot.width)), Image.LANCZOS)
    shot = shot.crop((0, 0, x1 - x0, min(shot.height, y1 - y0)))
    shadow = Image.new("RGBA", (shot.width + 40, shot.height + 40), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle((20, 20, shot.width + 20, shot.height + 20), radius=16, fill=(0, 0, 0, 170))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    base.paste(shadow, (x0 - 20, y0 - 12), shadow)
    mask = Image.new("L", shot.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, shot.width, shot.height), radius=14, fill=255)
    base.paste(shot, (x0, y0), mask)
    d = ImageDraw.Draw(base)
    f = font(20, 800)
    w = d.textlength(label, font=f)
    d.rounded_rectangle((x0 - 10, y0 - 22, x0 + w + 18, y0 + 14), radius=18, fill=color)
    d.text((x0 + 4, y0 - 18), label, font=f, fill=(255, 255, 255))


def main():
    r = load("live_live")
    det, stat, lat, rec = r["detection"], r["static_baseline_same_windows"], r["latency_seconds"], r["reconciliation"]
    bench = load("bench_throughput")
    best = max(bench["runs"], key=lambda x: x["rows_per_s"])

    im = background()
    card(im, SHOTS / "spark_streaming_silver.png", (930, 70, 1550, 330), "Spark Structured Streaming", ORANGE,
         crop=(0, 0, 1440, 640))
    card(im, IMG / "detection_traces.png", (960, 370, 1570, 620), "alerts vs ground truth", RED)
    card(im, SHOTS / "kafka_ui_messages.png", (900, 660, 1420, 860), "Kafka · cell.kpi", ICE)

    d = ImageDraw.Draw(im)
    d.text((80, 70), "MASTERFABRIC", font=font(36, 800), fill=WHITE)
    d.text((76, 120), "Realtime Telco", font=font(92, 800), fill=WHITE)
    x, y = 80, 238
    for word, col in (("Kafka", ICE), (" · ", GREY), ("Spark Streaming", ORANGE), (" · ", GREY), ("Iceberg", PURPLE),
                      (" · ", GREY), ("anomalies", RED)):
        f = font(40, 800)
        d.text((x, y), word, font=f, fill=col)
        x += d.textlength(word, font=f)
    d.text((80, 318), "Live radio-network telemetry with late, duplicate and out-of-order events,",
           font=font(26, 500), fill=GREY)
    d.text((80, 352), "typed alerts scored against ground truth. Every number measured.", font=font(26, 500), fill=GREY)
    facts = [
        (f"{det['incidents_detected']}/{det['incidents']} incidents found · precision {det['precision']:.2f}",
         f"held-out seed · static thresholds on the same windows: precision {stat['precision']:.2f}", GREEN),
        (f"alert {lat['last_report_to_alert']['p50_s']:.1f} s after the last report (p50)",
         f"Kafka → watermark → silver window → Iceberg stream → detector → MERGE + Kafka", RED),
        (f"{rec['bronze_rows']:,} events · {rec['duplicates_removed']:,} duplicates removed",
         f"2-minute watermark · {r['queries']['silver']['dropped_by_watermark']} very late reports dropped, kept in bronze", ICE),
        (f"{best['rows_per_s']:,.0f} events/s drained on a laptop",
         f"Spark local[4] · 6 partitions · {best['label']}", YELLOW),
    ]
    y = 430
    for title, sub, col in facts:
        d.rounded_rectangle((80, y + 6, 88, y + 62), radius=4, fill=col)
        d.text((106, y), title, font=font(29, 750), fill=WHITE)
        d.text((106, y + 38), sub, font=font(20, 450), fill=GREY)
        y += 92
    d.text((80, 820), "github.com/masterfabric/masterfabric-realtime-telco", font=font(30, 700), fill=WHITE)
    im.save(DOCS / "hero.png", optimize=True)
    print("docs/hero.png written")


if __name__ == "__main__":
    main()
