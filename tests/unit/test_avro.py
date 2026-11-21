"""Avro wire format and schema evolution rules, without Kafka or the registry."""
import io
import json
import struct

import fastavro
import pytest

from producer.avro_codec import AvroEncoder, decode, load_schema
from producer.run import encode

REC = {"v": 1, "event_id": "C1-0000001", "event_ts": "2026-11-16T05:00:10.000Z", "cell_id": "C1", "site_id": "S1",
       "city": "IST", "rat": "NR", "interval_s": 10, "active_users": 10.5, "prb_util_pct": 40.0,
       "dl_throughput_mbps": 300.0, "ul_sinr_db": 12.0, "latency_ms": 15.0, "rrc_attempts": 9, "rrc_success": 9,
       "erab_drops": 0, "availability_pct": 100.0}


def test_wire_header_and_round_trip():
    v1 = load_schema("cell_kpi_v1")
    wire = AvroEncoder(v1, 42).encode(REC)
    assert wire[0] == 0 and struct.unpack(">I", wire[1:5])[0] == 42
    sid, back = decode(wire, {42: fastavro.parse_schema(v1)})
    assert sid == 42 and back["event_id"] == REC["event_id"] and back["prb_util_pct"] == 40.0
    assert back["event_ts"].isoformat().startswith("2026-11-16T05:00:10")


def test_avro_is_smaller_than_json():
    wire = AvroEncoder(load_schema("cell_kpi_v1"), 1).encode(REC)
    assert len(wire) < len(json.dumps(REC).encode()) / 2


def test_v1_data_reads_with_v2_reader():
    v1, v2 = load_schema("cell_kpi_v1"), load_schema("cell_kpi_v2")
    body = AvroEncoder(v1, 1).encode(REC)[5:]
    r = fastavro.schemaless_reader(io.BytesIO(body), fastavro.parse_schema(v1), fastavro.parse_schema(v2))
    assert r["energy_kwh"] is None and r["cell_id"] == "C1"


def test_v3_reader_cannot_read_v1_data():
    v1, v3 = load_schema("cell_kpi_v1"), load_schema("cell_kpi_v3_incompatible")
    body = AvroEncoder(v1, 1).encode(REC)[5:]
    with pytest.raises(Exception):
        fastavro.schemaless_reader(io.BytesIO(body), fastavro.parse_schema(v1), fastavro.parse_schema(v3))


def test_producer_switches_to_v2():
    class A:
        v2_from_minute = 30
    enc = {1: AvroEncoder(load_schema("cell_kpi_v1"), 1), 2: AvroEncoder(load_schema("cell_kpi_v2"), 2)}
    sizes = {"avro": 0, "json_equivalent": 0}
    assert struct.unpack(">I", encode(REC, 10, A, enc, sizes)[1:5])[0] == 1
    assert struct.unpack(">I", encode(REC, 30, A, enc, sizes)[1:5])[0] == 2
    assert sizes["avro"] < sizes["json_equivalent"]
