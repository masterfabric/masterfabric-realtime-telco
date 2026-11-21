"""Schema registry demo: register v1, enforce BACKWARD compatibility, evolve to v2, get v3 rejected.

Runs inside the Spark container against the lab registry and writes docs/results/schema_registry.json.
The subject is reset first so the run is repeatable (lab registry only).

    docker exec -w /lab mf-rt-spark python scripts/registry_demo.py
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fastavro  # noqa: E402

from producer.avro_codec import AvroEncoder, Registry, decode, load_schema  # noqa: E402

URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://registry:8080/apis/ccompat/v7")
SUBJECT = "cell.kpi.avro-value"


def timed(fn, *a):
    t = time.perf_counter()
    status, body = fn(*a)
    return status, body, round((time.perf_counter() - t) * 1000, 1)


def main() -> None:
    reg = Registry(URL)
    reg._call("DELETE", f"/subjects/{SUBJECT}")
    reg._call("DELETE", f"/subjects/{SUBJECT}?permanent=true")
    v1, v2, v3 = (load_schema(n) for n in ("cell_kpi_v1", "cell_kpi_v2", "cell_kpi_v3_incompatible"))
    steps = []

    def step(name, fn, *a):
        status, body, ms = timed(fn, *a)
        steps.append({"step": name, "http_status": status, "ms": ms, "response": body})
        print(f"{name:<34} {status} {ms:>6} ms  {json.dumps(body)[:160]}")
        return status, body

    _, r1 = step("register v1", reg.register, SUBJECT, v1)
    step("set subject compatibility BACKWARD", reg.set_compatibility, SUBJECT, "BACKWARD")
    step("check v2 (optional field + default)", reg.check, SUBJECT, v2)
    _, r2 = step("register v2", reg.register, SUBJECT, v2)
    step("check v3 (required field, no default)", reg.check, SUBJECT, v3)
    s3, _ = step("register v3", reg.register, SUBJECT, v3)
    step("re-register v1 (idempotent)", reg.register, SUBJECT, v1)
    step("list versions", reg.versions, SUBJECT)

    # Wire-format round trip and schema resolution: a v1 record read with the v2 reader schema.
    rec = {"v": 1, "event_id": "C0001-0000042", "event_ts": "2026-11-16T05:07:00.000Z", "cell_id": "C0001",
           "site_id": "S001", "city": "Istanbul", "rat": "NR", "interval_s": 10, "active_users": 182.4,
           "prb_util_pct": 61.25, "dl_throughput_mbps": 452.12, "ul_sinr_db": 13.4, "latency_ms": 17.85,
           "rrc_attempts": 91, "rrc_success": 90, "erab_drops": 0, "availability_pct": 100.0}
    enc = AvroEncoder(v1, r1["id"])
    wire = enc.encode(rec)
    sid, back = decode(wire, {r1["id"]: fastavro.parse_schema(v1)})
    as_v2 = fastavro.schemaless_reader(io.BytesIO(wire[5:]), fastavro.parse_schema(v1), fastavro.parse_schema(v2))
    out = {
        "registry": "Apicurio Registry 3.3.5, Confluent-compatible API /apis/ccompat/v7, kafkasql storage",
        "subject": SUBJECT,
        "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "steps": steps,
        "ids": {"v1": r1["id"], "v2": r2["id"]},
        "v3_rejected": s3 == 409,
        "sample_record_bytes": {"json": len(json.dumps(rec).encode()), "avro_wire": len(wire),
                                "avro_header": 5},
        "round_trip_equal": back["event_id"] == rec["event_id"] and back["prb_util_pct"] == rec["prb_util_pct"]
                            and sid == r1["id"],
        "v1_read_with_v2_reader": {"energy_kwh": as_v2.get("energy_kwh"), "cell_id": as_v2["cell_id"]},
    }
    os.makedirs("docs/results", exist_ok=True)
    with open("docs/results/schema_registry.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps({k: out[k] for k in ("ids", "v3_rejected", "sample_record_bytes", "round_trip_equal",
                                          "v1_read_with_v2_reader")}))


if __name__ == "__main__":
    main()
