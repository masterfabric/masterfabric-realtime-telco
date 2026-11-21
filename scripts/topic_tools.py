"""Kafka helpers for the engine comparison and the exactly-once tests (run inside the Spark container).

    python scripts/topic_tools.py recreate cell.kpi.avro:6 kpi_1m.spark:3
    python scripts/topic_tools.py tap --topics kpi_1m.spark,kpi_1m.flink --out /lab/.out/tap.jsonl --idle-s 30
    python scripts/topic_tools.py offsets cell.kpi.eo
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BOOT = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def recreate(specs: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": BOOT})
    names = [s.split(":")[0] for s in specs]
    existing = set(admin.list_topics(timeout=10).topics)
    victims = [n for n in names if n in existing]
    if victims:
        for f in admin.delete_topics(victims).values():
            f.result()
        for _ in range(60):                         # deletion is asynchronous on the broker
            if not set(victims) & set(admin.list_topics(timeout=10).topics):
                break
            time.sleep(0.5)
    new = [NewTopic(s.split(":")[0], int(s.split(":")[1]), 1, config={"retention.ms": "86400000"}) for s in specs]
    for _ in range(20):
        try:
            for f in admin.create_topics(new).values():
                f.result()
            break
        except Exception as e:                       # noqa: BLE001  topic still marked for deletion
            if "already exists" not in str(e) and "marked for deletion" not in str(e):
                raise
            time.sleep(1)
    print(json.dumps({"recreated": names}))


def end_offsets(topic: str) -> dict[int, int]:
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "offsets-probe", "enable.auto.commit": False,
                  "isolation.level": "read_committed"})
    md = c.list_topics(topic, timeout=10).topics[topic]
    out = {}
    for p in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        out[p] = hi
    c.close()
    return out


def tap(topics: list[str], out: str, idle_s: float, isolation: str, max_s: float, stop_file: str | None) -> None:
    """Consume from the beginning and log every record with the wall-clock time it became visible."""
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"tap-{time.time_ns()}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "isolation.level": isolation, "fetch.wait.max.ms": 50})
    parts = []
    for t in topics:                                   # assign, not subscribe: no group rebalance delay
        md = c.list_topics(t, timeout=10).topics[t]
        parts += [TopicPartition(t, p, 0) for p in md.partitions]
    c.assign(parts)
    n, last, t0 = 0, time.time(), time.time()
    with open(out, "w") as f:
        while True:
            msgs = c.consume(5000, 0.2)
            now = time.time()
            for m in msgs:
                if m.error():
                    continue
                f.write(json.dumps({"t": m.topic(), "p": m.partition(), "o": m.offset(),
                                    "k": (m.key() or b"").decode(), "v": json.loads(m.value()),
                                    "kts": m.timestamp()[1], "recv": int(now * 1000)}) + "\n")
                n += 1
            if msgs:
                last = now
            if stop_file and os.path.exists(stop_file) and now - last > idle_s:
                break
            if not stop_file and now - last > idle_s:
                break
            if now - t0 > max_s:
                break
    c.close()
    print(json.dumps({"tapped": n, "file": out}))


def input_index(topic: str, out: str) -> None:
    """Decode the Avro input topic: per (cell, window) the Kafka timestamp of its last record and the
    number of distinct event ids (ground truth for a window that ignores lateness)."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    import fastavro
    from producer.avro_codec import Registry, decode
    reg = Registry(os.environ.get("SCHEMA_REGISTRY_URL", "http://registry:8080/apis/ccompat/v7"))
    ends = end_offsets(topic)
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"idx-{time.time_ns()}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.assign([TopicPartition(topic, p, 0) for p in ends])
    schemas, idx, n, done = {}, {}, 0, set()
    while len(done) < len(ends):
        for m in c.consume(10000, 1.0):
            if m.error():
                continue
            sid = struct.unpack(">I", m.value()[1:5])[0]
            if sid not in schemas:
                schemas[sid] = fastavro.parse_schema(reg.schema_by_id(sid))
            _, r = decode(m.value(), schemas)
            ws = r["event_ts"].replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            k = f"{r['cell_id']}|{ws}"
            ts = m.timestamp()[1]
            e = idx.setdefault(k, [0, set()])
            e[0] = max(e[0], ts)
            e[1].add(r["event_id"])
            n += 1
            if m.offset() + 1 >= ends[m.partition()]:
                done.add(m.partition())
        if all(v == 0 for v in ends.values()):
            break
    c.close()
    with open(out, "w") as f:
        json.dump({k: {"last_kts": v[0], "events": len(v[1])} for k, v in idx.items()}, f)
    print(json.dumps({"input_records": n, "windows": len(idx), "file": out}))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("recreate"); r.add_argument("specs", nargs="+")
    o = sub.add_parser("offsets"); o.add_argument("topic")
    t = sub.add_parser("tap")
    t.add_argument("--topics", required=True); t.add_argument("--out", required=True)
    t.add_argument("--idle-s", type=float, default=30); t.add_argument("--max-s", type=float, default=3600)
    t.add_argument("--isolation", default="read_committed"); t.add_argument("--stop-file", default=None)
    i = sub.add_parser("input-index"); i.add_argument("topic"); i.add_argument("--out", required=True)
    a = p.parse_args()
    if a.cmd == "recreate":
        recreate(a.specs)
    elif a.cmd == "offsets":
        e = end_offsets(a.topic); print(json.dumps({"topic": a.topic, "end_offsets": e, "total": sum(e.values())}))
    elif a.cmd == "tap":
        tap(a.topics.split(","), a.out, a.idle_s, a.isolation, a.max_s, a.stop_file)
    else:
        input_index(a.topic, a.out)


if __name__ == "__main__":
    main()
