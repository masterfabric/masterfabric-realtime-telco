"""Turn the network model into 10-second KPI reports, one per cell, the way an OSS/PM feed would."""
from __future__ import annotations

import numpy as np

from producer.network import Cell, Incident, diurnal

SCHEMA_VERSION = 1


def kpi_report(cell: Cell, sim_minute: float, hour_of_day: float, interval_s: int,
               active: list[Incident], rng: np.random.Generator) -> dict:
    """One PM counter report for one cell over `interval_s` seconds."""
    kinds = {i.kind: i for i in active}
    demand = diurnal(hour_of_day, cell.profile) * cell.load_factor * 0.78
    if "flash_crowd" in kinds:
        demand *= 1.5
    if "congestion" in kinds:
        demand = max(demand * 1.9, 1.15)            # demand beyond what the cell can serve
    users = max(0.0, cell.capacity_users * demand * rng.normal(1.0, 0.06))

    # Normal operation keeps PRB below ~93%; only a real overload pins it at the ceiling.
    prb = min(100.0, 100.0 * min(users / cell.capacity_users, 1.0) * 0.92 + rng.normal(0, 2.0))
    if "congestion" in kinds:
        prb = min(100.0, 97.0 + abs(rng.normal(0, 1.2)))
    prb = max(0.0, prb)

    latency = 18.0 + 22.0 * (prb / 100.0) ** 4 + rng.normal(0, 1.5)
    if "congestion" in kinds:
        latency = 85.0 + 40.0 * rng.random()
    sinr = cell.sinr_base_db + rng.normal(0, 1.0)
    p_drop = 0.002
    p_rrc = 0.995
    if "interference" in kinds:
        ramp = min(1.0, kinds["interference"].progress(sim_minute) * 4)  # ramps up over the first quarter
        sinr -= 9.0 * ramp
        p_drop = 0.002 + 0.02 * ramp
        p_rrc = 0.995 - 0.015 * ramp
    if "degradation" in kinds:
        p_rrc = 0.995 - 0.09 * kinds["degradation"].progress(sim_minute)  # slow hardware fault
        p_drop = 0.002 + 0.006 * kinds["degradation"].progress(sim_minute)

    attempts = int(rng.poisson(max(users, 1.0) * 0.5 * interval_s / 10))
    eff = 0.82 if "interference" not in kinds else 0.55
    thr = cell.capacity_mbps * prb / 100.0 * eff * rng.normal(1.0, 0.04)
    availability = 100.0

    if "outage" in kinds:                       # cell is down; the OSS still reports its state
        users, prb, thr, attempts, availability = 0.0, 0.0, 0.0, 0, 0.0
        latency, sinr = 0.0, 0.0
        success, drops = 0, 0
    elif "sleeping_cell" in kinds:              # reports itself healthy, but serves nobody
        users, prb, thr = rng.random() * 2, rng.random() * 3, rng.random() * 0.5
        attempts = max(attempts, int(rng.poisson(6)))
        success = int(rng.binomial(attempts, 0.03))
        drops = 0
    else:
        success = int(rng.binomial(attempts, min(1.0, max(0.0, p_rrc))))
        drops = int(rng.binomial(max(int(users * 0.3), 0), p_drop))

    return {
        "v": SCHEMA_VERSION,
        "cell_id": cell.cell_id,
        "site_id": cell.site_id,
        "city": cell.city,
        "rat": cell.rat,
        "interval_s": interval_s,
        "active_users": round(float(users), 1),
        "prb_util_pct": round(float(prb), 2),
        "dl_throughput_mbps": round(float(max(thr, 0.0)), 2),
        "ul_sinr_db": round(float(sinr), 2),
        "latency_ms": round(float(max(latency, 0.0)), 2),
        "rrc_attempts": attempts,
        "rrc_success": success,
        "erab_drops": drops,
        "availability_pct": availability,
    }
