from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from streaming import detector
from streaming.detector import CellState, step
from streaming.scoring import score

T0 = datetime(2026, 11, 16, 5, 0)


def window(i, rng, prb=50.0, latency=20.0, sinr=14.0, attempts=120, p_fail=0.005, availability=100.0):
    return {"cell_id": "IST-0001-1", "window_start": str(T0 + timedelta(minutes=i)),
            "prb_avg": prb + rng.normal(0, 1), "latency_avg": latency + rng.normal(0, 0.6),
            "sinr_avg": sinr + rng.normal(0, 0.4), "rrc_attempts": attempts,
            "rrc_success": attempts - int(rng.binomial(attempts, p_fail)), "availability_min": availability}


def run(windows):
    st, events = CellState(), []
    for w in windows:
        events += step(st, w)
    return st, events


def test_quiet_cell_never_alerts():
    rng = np.random.default_rng(1)
    _, ev = run([window(i, rng) for i in range(400)])
    assert ev == []


def test_outage_opens_immediately_and_closes_after_recovery():
    rng = np.random.default_rng(2)
    ws = [window(i, rng) for i in range(60)]
    ws += [window(60 + i, rng, availability=0.0, attempts=0, prb=0, latency=0, sinr=0) for i in range(5)]
    ws += [window(65 + i, rng) for i in range(10)]
    _, ev = run(ws)
    assert [(e["alert_type"], e["status"]) for e in ev] == [("outage", "OPEN"), ("outage", "CLOSED")]
    assert ev[0]["window_start"] == ws[60]["window_start"]
    assert ev[1]["window_start"] == ws[67]["window_start"]   # third healthy window


def test_outage_during_warmup_is_still_caught():
    rng = np.random.default_rng(3)
    ws = [window(i, rng) for i in range(5)] + [window(5, rng, availability=0.0, attempts=0)]
    _, ev = run(ws)
    assert ev and ev[0]["alert_type"] == "outage"


def test_congestion_needs_two_windows_and_a_latency_jump():
    rng = np.random.default_rng(4)
    base = [window(i, rng, prb=80) for i in range(60)]
    one = base + [window(60, rng, prb=98, latency=100)] + [window(61 + i, rng, prb=80) for i in range(5)]
    assert run(one)[1] == []
    two = base + [window(60 + i, rng, prb=98, latency=100) for i in range(3)]
    ev = run(two)[1]
    assert ev[0]["alert_type"] == "congestion" and ev[0]["window_start"] == two[61]["window_start"]
    busy_but_fast = base + [window(60 + i, rng, prb=97, latency=21) for i in range(5)]
    assert run(busy_but_fast)[1] == []          # a full cell with normal latency is not an incident


def test_interference_uses_the_cells_own_baseline():
    rng = np.random.default_rng(5)
    good = [window(i, rng, sinr=20.0) for i in range(60)]
    ev = run(good + [window(60 + i, rng, sinr=12.0) for i in range(3)])[1]   # 12 dB is fine elsewhere
    assert ev and ev[0]["alert_type"] == "interference"


def test_slow_degradation_is_caught_by_cusum_and_baseline_does_not_absorb_it():
    rng = np.random.default_rng(6)
    ws = [window(i, rng) for i in range(60)]
    ws += [window(60 + i, rng, p_fail=0.005 + 0.09 * i / 45) for i in range(45)]
    st, ev = run(ws)
    assert ev and ev[0]["alert_type"] == "degradation"
    opened = int((pd.Timestamp(ev[0]["window_start"]) - pd.Timestamp(T0)).total_seconds() // 60)
    assert 60 < opened < 75
    assert st.rrc_fail.mean < 0.01                    # baseline frozen while degraded


def test_state_round_trips_through_json():
    rng = np.random.default_rng(7)
    st, _ = run([window(i, rng) for i in range(40)])
    again = CellState.from_json(st.to_json())
    assert again == st


def test_static_rules_fire_on_a_busy_but_healthy_cell():
    w = {"prb_avg": 88.0, "latency_avg": 24.0, "sinr_avg": 14.0, "rrc_attempts": 100,
         "rrc_success": 100, "availability_min": 100.0}
    assert detector.static_rules(w) == "congestion"


def test_scoring_counts_hits_misses_and_false_alerts():
    truth = {"incidents": [
        {"incident_id": "A", "cell_id": "c1", "kind": "outage", "start_min": 10, "end_min": 20},
        {"incident_id": "B", "cell_id": "c2", "kind": "congestion", "start_min": 30, "end_min": 40}],
        "benign": [{"incident_id": "E", "cell_id": "c3", "kind": "flash_crowd", "start_min": 0, "end_min": 45}]}
    opens = pd.DataFrame([
        {"cell_id": "c1", "alert_type": "outage", "window_start": T0 + timedelta(minutes=10)},
        {"cell_id": "c1", "alert_type": "outage", "window_start": T0 + timedelta(minutes=80)},
        {"cell_id": "c3", "alert_type": "congestion", "window_start": T0 + timedelta(minutes=5)}])
    s = score(opens, truth, T0)
    assert (s["true_alerts"], s["false_alerts"], s["false_alerts_on_flash_crowds"]) == (1, 2, 1)
    assert s["recall"] == 0.5 and s["per_kind"]["outage"]["median_delay_min"] == 1.0
