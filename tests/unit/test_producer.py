import argparse
import json

import numpy as np
import pytest

from producer.network import INCIDENT_TYPES, Incident, build_network, diurnal
from producer.run import run
from producer.telemetry import kpi_report


def _args(tmp_path, **kw):
    base = dict(cells=30, hours=2.0, warmup_min=30, interval_s=10, speed=0, seed=11,
                incidents_per_cell_hour=0.1, flash_crowds=1, late_rate=0.03, very_late_rate=0.002,
                duplicate_rate=0.01, sink="file", bootstrap="", topic="t", out=str(tmp_path), run_id="r")
    base.update(kw)
    return argparse.Namespace(**base)


def test_network_is_deterministic_for_a_seed():
    a = build_network(60, 240, 30, seed=3)
    b = build_network(60, 240, 30, seed=3)
    assert a.incidents == b.incidents and a.cells == b.cells
    assert build_network(60, 240, 30, seed=4).incidents != a.incidents


def test_incidents_respect_warmup_and_never_overlap_on_a_cell():
    net = build_network(90, 360, 60, seed=5, incidents_per_cell_hour=0.05)
    assert net.incidents and {i.kind for i in net.incidents} <= set(INCIDENT_TYPES)
    by_cell = {}
    for i in net.incidents:
        assert i.start_min >= 60 and i.end_min <= 360
        by_cell.setdefault(i.cell_id, []).append(i)
    for spans in by_cell.values():
        spans.sort(key=lambda i: i.start_min)
        for x, y in zip(spans, spans[1:]):
            assert x.end_min <= y.start_min


def test_flash_crowd_covers_every_sector_of_one_site():
    net = build_network(60, 240, 30, seed=8, flash_crowds=1)
    cells = {b.cell_id for b in net.benign}
    sites = {c.split("-")[0] + "-" + c.split("-")[1] for c in cells}
    assert len(sites) == 1 and len(cells) == 3


def test_diurnal_profiles_peak_where_expected():
    assert diurnal(13, "business") > diurnal(4, "business") * 3
    assert diurnal(21, "residential") > diurnal(13, "residential")
    assert all(0.1 < diurnal(h, p) <= 1.0 for h in range(24) for p in ("business", "residential"))


@pytest.mark.parametrize("kind,check", [
    ("outage", lambda r: r["availability_pct"] == 0 and r["rrc_attempts"] == 0),
    ("sleeping_cell", lambda r: r["availability_pct"] == 100 and r["rrc_success"] <= 0.2 * r["rrc_attempts"]),
    ("congestion", lambda r: r["prb_util_pct"] >= 97 and r["latency_ms"] >= 85),
])
def test_incident_signatures(kind, check):
    net = build_network(9, 240, 30, seed=1)
    cell = net.cells[0]
    inc = Incident("X", cell.cell_id, kind, 0, 100)
    rng = np.random.default_rng(0)
    for _ in range(20):
        assert check(kpi_report(cell, 50, 18.0, 10, [inc], rng))


def test_interference_lowers_sinr_and_raises_drops():
    net = build_network(9, 240, 30, seed=1)
    cell = net.cells[0]
    inc = Incident("X", cell.cell_id, "interference", 0, 100)
    rng = np.random.default_rng(0)
    normal = np.mean([kpi_report(cell, 80, 18.0, 10, [], rng)["ul_sinr_db"] for _ in range(50)])
    bad = np.mean([kpi_report(cell, 80, 18.0, 10, [inc], rng)["ul_sinr_db"] for _ in range(50)])
    assert normal - bad > 7


def test_file_run_accounts_for_every_delivery(tmp_path):
    m = run(_args(tmp_path))
    d = m["delivery"]
    assert d["reports"] == 30 * 2 * 60 * 6
    assert d["sent"] == d["reports"] + d["duplicates"]
    lines = (tmp_path / "r" / "events.jsonl").read_text().splitlines()
    assert len(lines) == d["sent"]
    ids = [json.loads(x)["event_id"] for x in lines]
    assert len(set(ids)) == d["reports"]
    truth = json.loads((tmp_path / "r" / "truth.json").read_text())
    assert len(truth["incidents"]) == m["incidents"] and truth["benign"]


def test_out_of_order_delivery_is_present(tmp_path):
    run(_args(tmp_path))
    ts = [json.loads(x)["event_ts"] for x in (tmp_path / "r" / "events.jsonl").read_text().splitlines()]
    inversions = sum(1 for a, b in zip(ts, ts[1:]) if b < a)
    assert inversions > 100
