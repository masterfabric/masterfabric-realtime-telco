"""Synthetic radio access network: sites, cells, load profiles and injected incidents.

Everything here is deterministic for a given seed, so a run can be replayed and the detector
scored against the exact ground truth that produced the telemetry.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

CITIES = [("IST", 0.60, 41.01, 28.97), ("ANK", 0.25, 39.93, 32.86), ("IZM", 0.15, 38.42, 27.14)]

# Incident types the detector is expected to find, and the benign event it must not alert on.
INCIDENT_TYPES = ("outage", "sleeping_cell", "congestion", "interference", "degradation")
BENIGN_TYPES = ("flash_crowd",)

# Duration ranges in minutes and relative frequency of each incident type.
INCIDENT_SPEC = {
    "outage": ((5, 20), 0.20),
    "sleeping_cell": ((10, 30), 0.15),
    "congestion": ((10, 40), 0.25),
    "interference": ((10, 30), 0.20),
    "degradation": ((30, 60), 0.20),
}


@dataclass(frozen=True)
class Cell:
    cell_id: str
    site_id: str
    city: str
    rat: str            # LTE or NR
    profile: str        # business (midday peak) or residential (evening peak)
    capacity_users: int
    capacity_mbps: float
    load_factor: float
    sinr_base_db: float


@dataclass(frozen=True)
class Incident:
    incident_id: str
    cell_id: str
    kind: str
    start_min: int      # minutes since simulation start
    end_min: int        # exclusive

    def active(self, minute: float) -> bool:
        return self.start_min <= minute < self.end_min

    def progress(self, minute: float) -> float:
        return (minute - self.start_min) / max(1, self.end_min - self.start_min)


@dataclass
class Network:
    cells: list[Cell]
    incidents: list[Incident] = field(default_factory=list)
    benign: list[Incident] = field(default_factory=list)

    def by_cell(self) -> dict[str, list[Incident]]:
        out: dict[str, list[Incident]] = {c.cell_id: [] for c in self.cells}
        for i in self.incidents + self.benign:
            out[i.cell_id].append(i)
        return out


def build_cells(n_cells: int, rng: np.random.Generator) -> list[Cell]:
    """Three sectors per site; sites spread over three cities by population share."""
    cells: list[Cell] = []
    n_sites = math.ceil(n_cells / 3)
    weights = np.array([c[1] for c in CITIES])
    site_cities = rng.choice(len(CITIES), size=n_sites, p=weights / weights.sum())
    for s in range(n_sites):
        city = CITIES[site_cities[s]][0]
        rat = "NR" if rng.random() < 0.35 else "LTE"
        profile = "business" if rng.random() < 0.4 else "residential"
        for sector in range(3):
            if len(cells) == n_cells:
                break
            cap_users = 300 if rat == "NR" else 150
            cells.append(Cell(
                cell_id=f"{city}-{s:04d}-{sector + 1}",
                site_id=f"{city}-{s:04d}",
                city=city,
                rat=rat,
                profile=profile,
                capacity_users=cap_users,
                capacity_mbps=float(900 if rat == "NR" else 150),
                load_factor=float(np.clip(rng.lognormal(0.0, 0.25), 0.45, 1.25)),
                sinr_base_db=float(np.clip(rng.normal(14.0, 3.0), 6.0, 22.0)),
            ))
    return cells


def schedule_incidents(cells: list[Cell], total_min: int, warmup_min: int, per_cell_hour: float,
                       rng: np.random.Generator) -> list[Incident]:
    """Poisson-ish incident schedule; incidents never overlap on the same cell and never start
    during the warm-up, when the detector has no baseline yet."""
    hours = (total_min - warmup_min) / 60
    n = int(round(len(cells) * hours * per_cell_hour))
    kinds = list(INCIDENT_SPEC)
    probs = np.array([INCIDENT_SPEC[k][1] for k in kinds])
    busy: dict[str, list[tuple[int, int]]] = {}
    out: list[Incident] = []
    attempts = 0
    while len(out) < n and attempts < n * 50:
        attempts += 1
        kind = kinds[rng.choice(len(kinds), p=probs / probs.sum())]
        lo, hi = INCIDENT_SPEC[kind][0]
        dur = int(rng.integers(lo, hi + 1))
        start = int(rng.integers(warmup_min, max(warmup_min + 1, total_min - dur - 5)))
        cell = cells[int(rng.integers(0, len(cells)))]
        span = (start - 15, start + dur + 15)  # keep a quiet gap so incidents stay separable
        if any(a < span[1] and span[0] < b for a, b in busy.get(cell.cell_id, [])):
            continue
        busy.setdefault(cell.cell_id, []).append(span)
        out.append(Incident(f"INC-{len(out) + 1:04d}", cell.cell_id, kind, start, start + dur))
    return sorted(out, key=lambda i: (i.start_min, i.cell_id))


def schedule_flash_crowds(cells: list[Cell], total_min: int, warmup_min: int, n_events: int,
                          busy: list[Incident], rng: np.random.Generator) -> list[Incident]:
    """A stadium or concert: every sector of one site carries ~1.5x load for 45 minutes.
    It is legitimate traffic, so any alert raised on it counts as a false positive."""
    sites = sorted({c.site_id for c in cells})
    taken = {i.cell_id for i in busy}
    out: list[Incident] = []
    for k in range(n_events):
        for _ in range(200):
            site = sites[int(rng.integers(0, len(sites)))]
            members = [c for c in cells if c.site_id == site]
            if any(c.cell_id in taken for c in members):
                continue
            start = int(rng.integers(warmup_min, total_min - 50))
            for c in members:
                out.append(Incident(f"EVT-{k + 1:02d}", c.cell_id, "flash_crowd", start, start + 45))
                taken.add(c.cell_id)
            break
    return out


def build_network(n_cells: int, total_min: int, warmup_min: int, seed: int,
                  incidents_per_cell_hour: float = 0.03, flash_crowds: int = 2) -> Network:
    rng = np.random.default_rng(seed)
    cells = build_cells(n_cells, rng)
    incidents = schedule_incidents(cells, total_min, warmup_min, incidents_per_cell_hour, rng)
    benign = schedule_flash_crowds(cells, total_min, warmup_min, flash_crowds, incidents, rng)
    return Network(cells, incidents, benign)


def diurnal(hour: float, profile: str) -> float:
    """Relative demand in [~0.15, 1.0]: night trough, business midday or residential evening peak."""
    base = 0.5 * (1 - math.cos(2 * math.pi * (hour - 4) / 24))
    peak_hour = 13.0 if profile == "business" else 21.0
    bump = math.exp(-((hour - peak_hour) ** 2) / 6.0)
    return min(1.0, 0.15 + 0.55 * base ** 1.3 + 0.30 * bump)
