"""Per-cell streaming anomaly detector.

The detector sees one finalised 1-minute KPI window per cell at a time, in event-time order, and
keeps a small state per cell (a few floats). It combines three kinds of evidence:

1. Hard rules for failures that need no baseline: the cell reports itself unavailable (outage), or
   it claims to be available while almost every connection attempt fails (sleeping cell).
2. Adaptive z-scores against an exponentially weighted baseline (mean and variance per KPI) for
   congestion (PRB at the ceiling and latency far above the cell's own normal) and interference
   (uplink SINR far below the cell's own normal). Each needs two consecutive windows.
3. A Bernoulli CUSUM (log-likelihood ratio of "failure rate p1" against "the cell's normal p0") on
   RRC setup failures, for slow degradations that a z-score would absorb. Failures are counts, so
   a count model is used instead of a Gaussian z-score: with ~100 attempts a minute and p0 of 0.5%,
   three failures in one window are rare but not alarming, and the CUSUM treats them that way.

The baseline is frozen while a window looks anomalous or an alert is open, so a fault does not
teach the detector that the fault is normal. Thresholds are per cell and follow the daily load
curve, which is why a static threshold set (see `static_rules`) does much worse on the same data.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field

ALPHA = 0.05                 # EWMA weight: ~20-minute memory
WARMUP_WINDOWS = 30          # adaptive rules stay silent until the baseline has 30 windows
SIGMA_FLOOR = {"prb": 2.0, "latency": 1.5, "sinr": 0.8, "rrc_fail": 0.004}
Z_CONGESTION = 4.0
PRB_CONGESTION = 95.0
Z_INTERFERENCE = -4.0
P0_FLOOR, P0_CAP = 0.002, 0.05   # bounds on the learned normal failure rate
CUSUM_H = 14.0               # decision interval in nats; chosen on the dev seed, see docs/results
CUSUM_CAP = 1.5              # x CUSUM_H: bounds how long a recovered cell stays "degraded"
CLEAR_WINDOWS = 3            # consecutive healthy windows that close an alert
SEVERITY = {"outage": "critical", "sleeping_cell": "critical", "congestion": "major",
            "interference": "major", "degradation": "minor"}


@dataclass
class Baseline:
    mean: float = 0.0
    var: float = 0.0
    n: int = 0

    def z(self, x: float, floor: float) -> float:
        return (x - self.mean) / max(math.sqrt(self.var), floor)

    def update(self, x: float) -> None:
        if self.n == 0:
            self.mean, self.var = x, 0.0
        else:
            d = x - self.mean
            self.mean += ALPHA * d
            self.var = (1 - ALPHA) * (self.var + ALPHA * d * d)
        self.n += 1


@dataclass
class CellState:
    windows: int = 0
    prb: Baseline = field(default_factory=Baseline)
    latency: Baseline = field(default_factory=Baseline)
    sinr: Baseline = field(default_factory=Baseline)
    rrc_fail: Baseline = field(default_factory=Baseline)
    cusum: float = 0.0
    streak: dict = field(default_factory=dict)     # condition -> consecutive windows
    open_type: str | None = None
    open_id: str | None = None
    open_since: str | None = None
    healthy_streak: int = 0
    last_window: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(s: str | None) -> "CellState":
        if not s:
            return CellState()
        d = json.loads(s)
        for k in ("prb", "latency", "sinr", "rrc_fail"):
            d[k] = Baseline(**d[k])
        return CellState(**d)


def features(w: dict) -> dict:
    attempts = w["rrc_attempts"]
    return {
        "availability": w["availability_min"],
        "prb": w["prb_avg"],
        "latency": w["latency_avg"],
        "sinr": w["sinr_avg"],
        "attempts": attempts,
        "rrc_fail": 1.0 - w["rrc_success"] / attempts if attempts > 0 else 0.0,
    }


def bernoulli_llr(state: CellState, f: dict) -> float:
    """Log-likelihood ratio of this window's failures under p1 (degraded) versus p0 (normal)."""
    p0 = min(max(state.rrc_fail.mean, P0_FLOOR), P0_CAP)
    p1 = min(0.5, max(4 * p0, p0 + 0.015))
    n = f["attempts"]
    fails = round(f["rrc_fail"] * n)
    return fails * math.log(p1 / p0) + (n - fails) * math.log((1 - p1) / (1 - p0))


def step(state: CellState, w: dict) -> list[dict]:
    """Consume one window (dict with the silver columns) and return alert events (OPEN/CLOSED)."""
    f = features(w)
    warm = state.windows >= WARMUP_WINDOWS
    z = {
        "prb": state.prb.z(f["prb"], SIGMA_FLOOR["prb"]),
        "latency": state.latency.z(f["latency"], SIGMA_FLOOR["latency"]),
        "sinr": state.sinr.z(f["sinr"], SIGMA_FLOOR["sinr"]),
        "rrc_fail": state.rrc_fail.z(f["rrc_fail"], SIGMA_FLOOR["rrc_fail"]),
    }

    # Evidence for this window.
    hits: dict[str, float] = {}
    if f["availability"] < 50.0:
        hits["outage"] = 100.0 - f["availability"]
    elif f["attempts"] >= 20 and f["rrc_fail"] > 0.5:
        hits["sleeping_cell"] = f["rrc_fail"] * 100
    if warm and "outage" not in hits and "sleeping_cell" not in hits:
        if f["prb"] >= PRB_CONGESTION and z["latency"] >= Z_CONGESTION:
            hits["congestion"] = z["latency"]
        if z["sinr"] <= Z_INTERFERENCE:
            hits["interference"] = -z["sinr"]
        if f["attempts"] > 0:
            # Capped: an uncapped CUSUM climbs to hundreds during a long degradation and then needs
            # hours of normal windows to drain, keeping the alert open long after recovery.
            state.cusum = min(CUSUM_CAP * CUSUM_H, max(0.0, state.cusum + bernoulli_llr(state, f)))
        if state.cusum >= CUSUM_H and "interference" not in hits:
            hits["degradation"] = state.cusum

    needed = {"outage": 1, "sleeping_cell": 1, "congestion": 2, "interference": 2, "degradation": 1}
    for k in needed:
        state.streak[k] = state.streak.get(k, 0) + 1 if k in hits else 0
    firing = [k for k in needed if state.streak[k] >= needed[k]]
    # Most severe first: an outage explains everything else on the cell.
    firing.sort(key=lambda k: list(needed).index(k))

    events: list[dict] = []
    ws = w["window_start"]

    def emit(status: str, kind: str, score: float) -> None:
        events.append({
            "alert_id": state.open_id, "cell_id": w["cell_id"], "alert_type": kind, "status": status,
            "severity": SEVERITY[kind], "window_start": ws, "opened_window": state.open_since,
            "score": round(float(score), 3),
            "evidence": json.dumps({k: round(v, 3) for k, v in f.items()} |
                                   {f"z_{k}": round(v, 2) for k, v in z.items()} |
                                   {"cusum": round(state.cusum, 2)}),
        })

    if firing:
        kind = firing[0]
        state.healthy_streak = 0
        if state.open_type != kind:
            if state.open_type is not None:
                emit("CLOSED", state.open_type, 0.0)
            state.open_type, state.open_since = kind, ws
            state.open_id = f"{w['cell_id']}@{ws}"
            emit("OPEN", kind, hits[kind])
    elif state.open_type is not None:
        state.healthy_streak += 1 if not hits else 0
        if state.healthy_streak >= CLEAR_WINDOWS:
            emit("CLOSED", state.open_type, 0.0)
            state.open_type = state.open_id = state.open_since = None
            state.healthy_streak = 0
            state.cusum = 0.0

    # Learn only from windows that look normal, so a fault never becomes the baseline.
    calm = not hits and state.open_type is None and abs(z["rrc_fail"]) < 3 and state.cusum < CUSUM_H / 2
    if calm or not warm:
        if f["availability"] >= 50.0 and not (f["attempts"] >= 20 and f["rrc_fail"] > 0.5):
            state.prb.update(f["prb"])
            state.latency.update(f["latency"])
            state.sinr.update(f["sinr"])
            if f["attempts"] > 0:
                state.rrc_fail.update(f["rrc_fail"])
            state.windows += 1
    state.last_window = ws
    return events


# --------------------------------------------------------------------------------------------
# Static-threshold baseline, the way many NOC dashboards still alarm. Used only for comparison.
STATIC = {"availability_lt": 50.0, "prb_gt": 85.0, "latency_gt": 30.0, "sinr_lt": 6.0, "rrc_sr_lt": 0.97}


def static_rules(w: dict) -> str | None:
    f = features(w)
    if f["availability"] < STATIC["availability_lt"]:
        return "outage"
    if f["attempts"] >= 20 and f["rrc_fail"] > 0.5:
        return "sleeping_cell"
    if f["prb"] > STATIC["prb_gt"] or f["latency"] > STATIC["latency_gt"]:
        return "congestion"
    if f["sinr"] < STATIC["sinr_lt"]:
        return "interference"
    if f["attempts"] > 0 and 1 - f["rrc_fail"] < STATIC["rrc_sr_lt"]:
        return "degradation"
    return None
