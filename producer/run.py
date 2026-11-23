"""Stream synthetic cell KPI reports into Kafka on a simulated clock.

The simulation clock runs `--speed` times faster than the wall clock, so six network hours can be
replayed in a few minutes while event-time semantics (windows, watermarks, lateness) stay intact.
Delivery is deliberately imperfect, like a real mediation layer:

- a share of reports arrives late and out of order (seconds to a minute),
- a small share arrives very late (5-10 minutes), beyond the streaming watermark,
- a small share is re-sent with the same event_id (at-least-once retries).

Ground truth (incidents, benign events, delivery counters) is written to data/runs/<run_id>/ so the
detector can be scored afterwards.

    python -m producer.run --cells 240 --hours 6 --speed 120
    python -m producer.run --cells 240 --hours 2 --speed 0 --topic cell.kpi.bench   # max rate
    python -m producer.run --format avro --topic cell.kpi.avro --v2-from-minute 180  # schema change mid-run
"""
from __future__ import annotations

import argparse
import heapq
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from producer.network import build_network
from producer.telemetry import kpi_report

SIM_START = datetime(2026, 11, 16, 5, 0, tzinfo=timezone.utc)  # Monday 08:00 Istanbul time (UTC+3): the morning ramp
LOCAL_UTC_OFFSET_H = 3


class FileSink:
    """Used by tests and dry runs: writes newline-delimited JSON instead of producing to Kafka."""

    def __init__(self, path: Path, arrivals: Path | None = None):
        self.f = open(path, "w")
        self.a = open(arrivals, "w") if arrivals else None   # simulated arrival second, one per line

    def send(self, key: str, value: bytes, at: float | None = None) -> None:
        self.f.write(value.decode() + "\n")
        if self.a is not None:
            self.a.write(f"{at:.3f}\n")

    def poll(self) -> None:
        pass

    def close(self) -> None:
        self.f.close()
        if self.a is not None:
            self.a.close()


class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer

        self.topic = topic
        self.errors = 0
        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,      # broker-side dedupe of producer retries
            "acks": "all",
            "linger.ms": 20,
            "batch.num.messages": 5000,
            "compression.type": "zstd",
            "queue.buffering.max.messages": 500000,
        })

    def _cb(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1

    def send(self, key: str, value: bytes, at: float | None = None) -> None:
        while True:
            try:
                self.p.produce(self.topic, key=key.encode(), value=value, on_delivery=self._cb)
                return
            except BufferError:
                self.p.poll(0.05)

    def poll(self) -> None:
        self.p.poll(0)

    def close(self) -> None:
        self.p.flush(60)


def setup_avro(args: argparse.Namespace):
    """Register v1 (and v2 when the run switches mid-way) and build one encoder per version."""
    from producer.avro_codec import AvroEncoder, Registry, load_schema

    reg = Registry(args.registry)
    subject = f"{args.topic}-value"                       # TopicNameStrategy, the Confluent default
    encoders, ids = {}, {}
    for ver in (1, 2) if args.v2_from_minute is not None else (1,):
        schema = load_schema(f"cell_kpi_v{ver}")
        status, body = reg.register(subject, schema)
        if status != 200:
            raise SystemExit(f"schema v{ver} rejected by the registry: {status} {body}")
        ids[f"v{ver}"] = body["id"]
        encoders[ver] = AvroEncoder(schema, body["id"])
    return encoders, ids, {"avro": 0, "json_equivalent": 0}


def encode(rec: dict, minute: float, args: argparse.Namespace, encoders, sizes: dict) -> bytes:
    if encoders is None:
        return json.dumps(rec).encode()
    if args.v2_from_minute is not None and minute >= args.v2_from_minute:
        rec = dict(rec, v=2, energy_kwh=round(0.25 + 0.004 * rec["prb_util_pct"], 4))   # synthetic site energy
        enc = encoders[2]
    else:
        enc = encoders[1]
    out = enc.encode(rec)
    sizes["avro"] += len(out)
    sizes["json_equivalent"] += len(json.dumps(rec).encode())
    return out


def run(args: argparse.Namespace) -> dict:
    total_min = int(args.hours * 60)
    net = build_network(args.cells, total_min, args.warmup_min, args.seed,
                        args.incidents_per_cell_hour, args.flash_crowds)
    by_cell = net.by_cell()
    rng = np.random.default_rng(args.seed + 1)
    run_id = args.run_id or f"run-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    out_dir = Path(args.out) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    arrivals = out_dir / "arrivals.txt" if getattr(args, "arrivals", False) else None
    sink = FileSink(out_dir / "events.jsonl", arrivals) if args.sink == "file" else KafkaSink(args.bootstrap, args.topic)
    encoders, schema_ids, sizes = setup_avro(args) if getattr(args, "format", "json") == "avro" else (None, {}, {})
    interval = args.interval_s
    ticks = total_min * 60 // interval
    pending: list[tuple[float, int, str, bytes]] = []   # (emit_at_sim_s, seq, key, payload)
    counters = {"reports": 0, "sent": 0, "late": 0, "very_late": 0, "duplicates": 0}
    seq = 0
    wall_start = time.time()

    def emit_due(now_sim_s: float) -> None:
        while pending and pending[0][0] <= now_sim_s:
            at, _, key, payload = heapq.heappop(pending)
            sink.send(key, payload, at)
            counters["sent"] += 1

    for tick in range(ticks):
        t_sim = tick * interval
        if args.speed > 0:                                   # pace the wall clock to the sim clock
            target = wall_start + t_sim / args.speed
            delay = target - time.time()
            if delay > 0:
                time.sleep(delay)
        minute = t_sim / 60
        ts = SIM_START + timedelta(seconds=t_sim)
        hour = (ts.hour + LOCAL_UTC_OFFSET_H + ts.minute / 60) % 24  # load follows local time
        for cell in net.cells:
            active = [i for i in by_cell[cell.cell_id] if i.active(minute)]
            rec = kpi_report(cell, minute, hour, interval, active, rng)
            rec["event_id"] = f"{cell.cell_id}-{tick:07d}"
            rec["event_ts"] = ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
            counters["reports"] += 1
            # Delivery delay in simulated seconds.
            u = rng.random()
            if u < args.very_late_rate:
                lag = float(rng.uniform(300, 600)); counters["very_late"] += 1
            elif u < args.very_late_rate + args.late_rate:
                lag = float(rng.uniform(5, 60)); counters["late"] += 1
            else:
                lag = float(rng.uniform(0, 2))
            copies = 2 if rng.random() < args.duplicate_rate else 1
            counters["duplicates"] += copies - 1
            for c in range(copies):
                emit_at = t_sim + lag + c * float(rng.uniform(1, 20))
                seq += 1
                heapq.heappush(pending, (emit_at, seq, cell.cell_id, encode(rec, minute, args, encoders, sizes)))
        emit_due(t_sim + interval)
        sink.poll()
    emit_due(float("inf"))
    sink.close()
    wall = time.time() - wall_start

    manifest = {
        "run_id": run_id,
        "topic": args.topic,
        "sim_start": SIM_START.isoformat(),
        "sim_minutes": total_min,
        "warmup_min": args.warmup_min,
        "interval_s": interval,
        "speed": args.speed,
        "wall_start_epoch": wall_start,
        "wall_seconds": round(wall, 2),
        "events_per_wall_second": round(counters["sent"] / wall, 1) if wall else None,
        "cells": len(net.cells),
        "seed": args.seed,
        "delivery": counters,
        "rates": {"late": args.late_rate, "very_late": args.very_late_rate, "duplicate": args.duplicate_rate},
        "producer_errors": getattr(sink, "errors", 0),
        "format": getattr(args, "format", "json"),
        "schema_ids": schema_ids,
        "payload_bytes": sizes,
        "incidents": len(net.incidents),
        "benign_events": len({b.incident_id for b in net.benign}),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (out_dir / "truth.json").write_text(json.dumps({
        "incidents": [asdict(i) for i in net.incidents],
        "benign": [asdict(b) for b in net.benign],
        "cells": [asdict(c) for c in net.cells],
    }, indent=1))
    return manifest


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cells", type=int, default=240)
    p.add_argument("--hours", type=float, default=6.0)
    p.add_argument("--warmup-min", type=int, default=60)
    p.add_argument("--interval-s", type=int, default=10)
    p.add_argument("--speed", type=float, default=120.0, help="sim seconds per wall second; 0 = as fast as possible")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--incidents-per-cell-hour", type=float, default=0.03)
    p.add_argument("--flash-crowds", type=int, default=2)
    p.add_argument("--late-rate", type=float, default=0.03)
    p.add_argument("--very-late-rate", type=float, default=0.001)
    p.add_argument("--duplicate-rate", type=float, default=0.005)
    p.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    p.add_argument("--arrivals", action="store_true",
                   help="file sink only: also write arrivals.txt, the simulated arrival second of each line")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="cell.kpi")
    p.add_argument("--format", choices=["json", "avro"], default="json")
    p.add_argument("--registry", default=os.environ.get("SCHEMA_REGISTRY_URL", "http://registry:8080/apis/ccompat/v7"))
    p.add_argument("--v2-from-minute", type=float, default=None,
                   help="avro only: switch to schema v2 (adds energy_kwh) from this simulated minute")
    p.add_argument("--out", default="/data/runs")
    p.add_argument("--run-id", default=None)
    m = run(p.parse_args())
    print(json.dumps({k: m[k] for k in ("run_id", "wall_seconds", "events_per_wall_second", "delivery",
                                        "incidents", "benign_events")}))


if __name__ == "__main__":
    main()
