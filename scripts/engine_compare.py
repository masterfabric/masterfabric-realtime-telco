"""Spark Structured Streaming vs Flink on the same Avro topic, and the exactly-once kill tests.

Runs on the host and drives the containers (`make schema-up flink-up` first). Each stage writes one
JSON file to docs/results/ and keeps raw taps in .out/cmp/.

    python3 scripts/engine_compare.py drain      # throughput on a pre-filled topic, 3 runs per engine
    python3 scripts/engine_compare.py live       # end-to-end latency at a steady rate, both engines at once
    python3 scripts/engine_compare.py eo-flink   # TaskManager kill -9: exactly-once vs at-least-once sink
    python3 scripts/engine_compare.py eo-spark   # driver kill -9: Iceberg sink vs Kafka sink

Only the standard library is used on the host; Kafka work happens in the Spark container.
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".out/cmp"
RES = ROOT / "docs/results"
REST = "http://127.0.0.1:28081"
SPARK = ["docker", "exec", "-w", "/lab", "mf-rt-spark"]


def sh(cmd: list[str], check: bool = True) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"failed: {' '.join(cmd)}\n{r.stdout[-2000:]}\n{r.stderr[-3000:]}")
    return r.stdout


def last_json(text: str) -> dict:
    for line in reversed(text.strip().splitlines()):
        if line.startswith("{"):
            return json.loads(line)
    raise ValueError(text[-500:])


def tools(*args: str) -> dict:
    return last_json(sh(SPARK + ["python", "scripts/topic_tools.py", *args]))


def produce(run_id: str, topic: str, hours: float, speed: float, v2_from: float | None, seed: int = 2026,
            fmt: str = "avro", warmup: int = 30) -> dict:
    cmd = SPARK + ["python", "-m", "producer.run", "--format", fmt, "--topic", topic, "--cells", "240",
                   "--hours", str(hours), "--speed", str(speed), "--seed", str(seed), "--warmup-min", str(warmup),
                   "--run-id", run_id]
    if v2_from is not None:
        cmd += ["--v2-from-minute", str(v2_from)]
    return last_json(sh(cmd))


def rest(path: str, method: str = "GET") -> dict:
    with urllib.request.urlopen(urllib.request.Request(REST + path, method=method), timeout=10) as r:
        b = r.read()
        return json.loads(b) if b else {}


def flink_submit(name: str, out_topic: str, guarantee: str = "at-least-once", checkpoint: str = "10 s",
                 bounded: bool = False, wait: bool = False) -> dict:
    cmd = ["python3", "scripts/flink_job.py", "--name", name, "--out-topic", out_topic, "--guarantee", guarantee,
           "--checkpoint", checkpoint]
    cmd += ["--bounded"] * bounded + ["--wait"] * wait
    return last_json(sh(cmd))


def flink_cancel(jid: str) -> None:
    rest(f"/jobs/{jid}?mode=cancel", "PATCH")


def flink_running(jid: str) -> bool:
    j = rest(f"/jobs/{jid}")
    return j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"])


class Stats(threading.Thread):
    """Samples `docker stats` for the engine containers until stopped."""

    def __init__(self, names: list[str]):
        super().__init__(daemon=True)
        self.names, self.samples, self.stop_evt = names, [], threading.Event()

    def run(self) -> None:
        while not self.stop_evt.is_set():
            out = sh(["docker", "stats", "--no-stream", "--format", "{{json .}}", *self.names], check=False)
            t = time.time()
            for line in out.splitlines():
                d = json.loads(line)
                mem = d["MemUsage"].split("/")[0].strip()
                mib = float(mem[:-3]) * (1024 if mem.endswith("GiB") else 1) if mem[-3:] in ("GiB", "MiB") else 0.0
                self.samples.append({"t": t, "name": d["Name"], "cpu": float(d["CPUPerc"].rstrip("%")), "mem_mib": mib})

    def summary(self) -> dict:
        self.stop_evt.set()
        self.join(timeout=10)
        out = {}
        for n in self.names:
            s = [x for x in self.samples if x["name"] == n]
            if s:
                out[n] = {"samples": len(s), "cpu_avg_pct": round(statistics.mean(x["cpu"] for x in s), 1),
                          "cpu_max_pct": round(max(x["cpu"] for x in s), 1),
                          "mem_max_mib": round(max(x["mem_mib"] for x in s), 1)}
        return out


def read_tap(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.open()] if path.exists() else []


def by_topic(rows: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["t"], []).append(r)
    return out


def key(v: dict) -> str:
    return f"{v['cell_id']}|{v['window_start']}"


def compare_outputs(a: dict[str, dict], b: dict[str, dict]) -> dict:
    common = set(a) & set(b)
    exact = sum(1 for k in common if a[k]["reports"] == b[k]["reports"]
                and a[k]["rrc_attempts"] == b[k]["rrc_attempts"] and a[k]["erab_drops"] == b[k]["erab_drops"]
                and a[k]["v2_reports"] == b[k]["v2_reports"]
                and abs(a[k]["prb_avg"] - b[k]["prb_avg"]) < 1e-9
                and abs((a[k].get("energy_kwh_sum") or 0) - (b[k].get("energy_kwh_sum") or 0)) < 1e-9)
    return {"common_windows": len(common), "identical": exact, "differ": len(common) - exact,
            "only_first": len(set(a) - set(b)), "only_second": len(set(b) - set(a))}


def vs_truth(out: dict[str, dict], truth: dict[str, dict]) -> dict:
    full = sum(1 for k, v in out.items() if k in truth and v["reports"] == truth[k]["events"])
    short = sum(truth[k]["events"] - v["reports"] for k, v in out.items() if k in truth)
    return {"windows": len(out), "complete_windows": full, "reports_missing_late": short,
            "truth_windows": len(truth)}


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1))))], 3)


# ----------------------------------------------------------------------------------------------- drain
def stage_drain(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tools("recreate", "cell.kpi.avro:6")
    prod = produce("cmp-drain", "cell.kpi.avro", a.hours, 0, a.hours * 30)
    idx = tools("input-index", "cell.kpi.avro", "--out", "/lab/.out/cmp/drain_idx.json")
    runs = []
    for i in range(a.repeats):
        for engine in ("spark", "flink"):
            tools("recreate", f"kpi_1m.{engine}:3")
            if engine == "spark":
                st = Stats(["mf-rt-spark"]); st.start()
                t0 = time.time()
                r = last_json(sh(SPARK + ["spark-submit", "--master", "local[6]", "--driver-memory", "3g",
                                          "streaming/avro_silver.py", "--available-now", "--reset",
                                          "--max-offsets", str(a.max_offsets)]))
                total = time.time() - t0
                runs.append({"engine": "spark", "run": i + 1, "processing_s": r["wall_s"],
                             "with_startup_s": round(total, 2), "batches": r["batches"], "stats": st.summary()})
            else:
                st = Stats(["mf-rt-flink-jm", "mf-rt-flink-tm"]); st.start()
                t0 = time.time()
                r = flink_submit(f"drain-{int(time.time())}", "kpi_1m.flink", bounded=True, wait=True)
                total = time.time() - t0
                runs.append({"engine": "flink", "run": i + 1, "processing_s": r["job_duration_s"],
                             "with_startup_s": round(total, 2), "state": r["state"], "stats": st.summary()})
            print(json.dumps(runs[-1]))
    tap = OUT / "drain_tap.jsonl"
    tools("tap", "--topics", "kpi_1m.spark,kpi_1m.flink", "--out", f"/lab/.out/cmp/{tap.name}", "--idle-s", "5")
    t = by_topic(read_tap(tap))
    S = {key(r["v"]): r["v"] for r in t.get("kpi_1m.spark", [])}
    F = {key(r["v"]): r["v"] for r in t.get("kpi_1m.flink", [])}
    truth = json.loads((OUT / "drain_idx.json").read_text())
    n = prod["delivery"]["sent"]
    summ = {}
    for e in ("spark", "flink"):
        rs = [r for r in runs if r["engine"] == e]
        med = statistics.median(r["processing_s"] for r in rs)
        summ[e] = {"processing_s_median": round(med, 2), "records_per_s": round(n / med),
                   "with_startup_s_median": round(statistics.median(r["with_startup_s"] for r in rs), 2)}
    res = {"input": {"records": n, "cells": 240, "sim_hours": a.hours, "v2_from_minute": a.hours * 30,
                     "late": prod["delivery"]["late"], "very_late": prod["delivery"]["very_late"],
                     "duplicates": prod["delivery"]["duplicates"], "windows_truth": idx["windows"]},
           "setup": {"spark": f"local[6], driver 3g, availableNow, maxOffsetsPerTrigger {a.max_offsets}, "
                              "6 shuffle partitions, HDFS state store",
                     "flink": "session cluster, 1 TaskManager (3 GB), parallelism 6, bounded latest-offset, "
                              "checkpoint 10 s"},
           "runs": runs, "summary": summ, "spark_vs_flink": compare_outputs(S, F),
           "spark_vs_truth": vs_truth(S, truth), "flink_vs_truth": vs_truth(F, truth)}
    (RES / "engine_drain.json").write_text(json.dumps(res, indent=2))
    print(json.dumps({k: res[k] for k in ("summary", "spark_vs_flink", "spark_vs_truth", "flink_vs_truth")}))


# ------------------------------------------------------------------------------------------------ live
def stage_live(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tools("recreate", "cell.kpi.avro:6", "kpi_1m.spark:3", "kpi_1m.flink:3", "kpi_1m.flink_eo:3")
    stop = "/lab/.out/cmp/live.stop"
    sh(SPARK + ["rm", "-f", stop])
    alo = flink_submit("live-alo", "kpi_1m.flink", "at-least-once", a.checkpoint)
    eo = flink_submit("live-eo", "kpi_1m.flink_eo", "exactly-once", a.checkpoint)
    spark = subprocess.Popen(SPARK + ["spark-submit", "--master", "local[6]", "--driver-memory", "3g",
                                      "streaming/avro_silver.py", "--reset", "--trigger-s", "1",
                                      "--idle-stop-s", "40"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    tap = subprocess.Popen(SPARK + ["python", "scripts/topic_tools.py", "tap", "--topics",
                                    "kpi_1m.spark,kpi_1m.flink,kpi_1m.flink_eo", "--out", "/lab/.out/cmp/live_tap.jsonl",
                                    "--idle-s", "15", "--stop-file", stop], stdout=subprocess.PIPE, text=True)
    time.sleep(35)                                         # let Spark plan and run its first empty batches
    st = Stats(["mf-rt-spark", "mf-rt-flink-tm"]); st.start()
    prod = produce("cmp-live", "cell.kpi.avro", a.hours, a.speed, a.hours * 30)
    spark.wait()
    stats = st.summary()
    time.sleep(20)
    for j in (alo, eo):
        flink_cancel(j["job_id"])
    sh(SPARK + ["touch", stop])
    tap.wait()
    tools("input-index", "cell.kpi.avro", "--out", "/lab/.out/cmp/live_idx.json")
    truth = json.loads((OUT / "live_idx.json").read_text())
    t = by_topic(read_tap(OUT / "live_tap.jsonl"))
    lat, outs = {}, {}
    for topic, label in (("kpi_1m.spark", "spark"), ("kpi_1m.flink", "flink_at_least_once"),
                         ("kpi_1m.flink_eo", "flink_exactly_once")):
        rows = t.get(topic, [])
        first: dict[str, dict] = {}
        for r in rows:
            first.setdefault(key(r["v"]), r)
        outs[label] = {k: r["v"] for k, r in first.items()}
        d = [(r["recv"] - truth[k]["last_kts"]) / 1000 for k, r in first.items() if k in truth]
        lat[label] = {"windows": len(first), "rows": len(rows), "p50_s": pct(d, 50), "p95_s": pct(d, 95),
                      "p99_s": pct(d, 99), "max_s": pct(d, 100)}
    floor = round(2 * 60 / a.speed, 2)
    res = {"input": {"records": prod["delivery"]["sent"], "speed": a.speed, "sim_hours": a.hours,
                     "events_per_s": prod["events_per_wall_second"]},
           "definition": "latency = wall time the output row became visible to a read_committed consumer minus "
                         "the Kafka timestamp of the last input record of that (cell, minute) window; it includes "
                         f"the 2-minute watermark, which at speed {a.speed} is {floor} s of wall time",
           "watermark_floor_s": floor, "flink_checkpoint": a.checkpoint, "spark_trigger_s": 1,
           "latency": lat, "stats_during_produce": stats,
           "spark_vs_flink_alo": compare_outputs(outs["spark"], outs["flink_at_least_once"]),
           "flink_alo_vs_eo": compare_outputs(outs["flink_at_least_once"], outs["flink_exactly_once"])}
    (RES / "engine_live.json").write_text(json.dumps(res, indent=2))
    print(json.dumps({k: res[k] for k in ("latency", "spark_vs_flink_alo", "flink_alo_vs_eo")}))


# -------------------------------------------------------------------------------------------- eo-flink
def wait_recovered(jids: list[str], t_kill: float, timeout: float = 180) -> float | None:
    time.sleep(1)
    while time.time() - t_kill < timeout:
        try:
            if all(flink_running(j) for j in jids):
                return round(time.time() - t_kill, 1)
        except Exception:                                   # noqa: BLE001  REST briefly unavailable
            pass
        time.sleep(0.5)
    return None


def stage_eo_flink(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    tools("recreate", "cell.kpi.avro:6", "eo.flink.exactly_once:3", "eo.flink.at_least_once:3")
    eo = flink_submit("eo-exactly", "eo.flink.exactly_once", "exactly-once", a.checkpoint)
    alo = flink_submit("eo-atleast", "eo.flink.at_least_once", "at-least-once", a.checkpoint)
    jids = [eo["job_id"], alo["job_id"]]
    time.sleep(10)
    prod_t = threading.Thread(target=lambda: produce("cmp-eo-flink", "cell.kpi.avro", a.hours, a.speed, None))
    t_start = time.time()
    prod_t.start()
    kills = []
    for at in a.kill_at:
        time.sleep(max(0.0, t_start + at - time.time()))
        t_kill = time.time()
        sh(["docker", "kill", "-s", "KILL", "mf-rt-flink-tm"])
        time.sleep(a.down_s)
        sh(["docker", "start", "mf-rt-flink-tm"])
        kills.append({"at_s": at, "recovered_after_s": wait_recovered(jids, t_kill)})
        print(json.dumps(kills[-1]))
    prod_t.join()
    time.sleep(45)                                          # last windows close via the idle-source watermark
    restarts = {}
    for name, j in (("exactly_once", eo["job_id"]), ("at_least_once", alo["job_id"])):
        ex = rest(f"/jobs/{j}/exceptions")
        restarts[name] = len(ex.get("exceptionHistory", {}).get("entries", []))
        flink_cancel(j)
    time.sleep(5)
    res = {"setup": {"checkpoint": a.checkpoint, "kills_at_s": a.kill_at, "taskmanager_down_s": a.down_s,
                     "speed": a.speed, "sim_hours": a.hours},
           "kills": kills, "job_failures_recorded": restarts, "topics": {}}
    for iso in ("read_committed", "read_uncommitted"):
        name = f"eo_flink_tap_{iso}.jsonl"
        tools("tap", "--topics", "eo.flink.exactly_once,eo.flink.at_least_once", "--out", f"/lab/.out/cmp/{name}",
              "--idle-s", "6", "--isolation", iso)
        for topic, rows in by_topic(read_tap(OUT / name)).items():
            keys = [key(r["v"]) for r in rows]
            res["topics"].setdefault(topic, {})[iso] = {"rows": len(rows), "distinct_windows": len(set(keys)),
                                                       "duplicate_rows": len(keys) - len(set(keys))}
    (RES / "exactly_once_flink.json").write_text(json.dumps(res, indent=2))
    print(json.dumps(res["topics"]))


# -------------------------------------------------------------------------------------------- eo-spark
def commits(path: str) -> int:
    out = sh(SPARK + ["bash", "-c", f"ls {path}/commits 2>/dev/null | grep -c '^[0-9]*$' || true"], check=False)
    return int(out.strip() or 0)


def spark_kill_loop(cmd_first: list[str], cmd_next: list[str], ckpt: str, kills: int, pattern: str) -> list[dict]:
    """Start a Spark job, kill -9 its driver `kills` times at random points, restart, let it finish."""
    rng = random.Random(42)
    events = []
    proc = subprocess.Popen(SPARK + cmd_first, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for i in range(kills):
        base = commits(ckpt)
        while commits(ckpt) < base + rng.randint(3, 6):     # a few batches into this attempt
            time.sleep(0.3)
        time.sleep(rng.uniform(0, 1.5))                     # land anywhere inside a batch
        before = commits(ckpt)
        sh(SPARK + ["pkill", "-9", "-f", pattern], check=False)
        proc.wait()
        t_restart = time.time()
        proc = subprocess.Popen(SPARK + cmd_next, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        while commits(ckpt) <= before and proc.poll() is None:
            time.sleep(0.2)
        events.append({"kill": i + 1, "commits_before_kill": before,
                       "restart_to_next_commit_s": round(time.time() - t_restart, 1)})
        print(json.dumps(events[-1]))
    proc.wait()
    return events


def stage_eo_spark(a: argparse.Namespace) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    res = {}
    # (1) Kafka -> Iceberg bronze (checkpoint + Iceberg commit), JSON topic, driver killed 3 times.
    tools("recreate", "cell.kpi.eo:6")
    prod = produce("cmp-eo-spark", "cell.kpi.eo", a.hours, 0, None, seed=7, fmt="json", warmup=60)
    ends = tools("offsets", "cell.kpi.eo")
    base = ["spark-submit", "--master", "local[4]", "streaming/app.py", "--run-id", "eo", "--ns", "eo",
            "--topic", "cell.kpi.eo", "--queries", "bronze", "--available-now", "--max-offsets", str(a.max_offsets),
            "--trigger-bronze", "1"]
    ev = spark_kill_loop(base + ["--reset"], base, "/checkpoints/eo/eo/bronze", a.kills, "streaming/app.py --run-id eo")
    chk = last_json(sh(SPARK + ["spark-submit", "--master", "local[2]", "streaming/eo_check.py", "--table",
                                "lake.eo.kpi_events"]))
    res["iceberg_sink"] = {"records_in_topic": ends["total"], "produced": prod["delivery"]["sent"],
                           "kills": ev, **chk,
                           "exactly_once": chk["rows"] == ends["total"] == chk["distinct_offsets"]}
    # (2) Kafka sink from Spark (at-least-once by design), same kill pattern on the Avro aggregation.
    tools("recreate", "eo.spark.kafka:3")
    cmd = ["spark-submit", "--master", "local[6]", "streaming/avro_silver.py", "--available-now",
           "--out-topic", "eo.spark.kafka", "--checkpoint", "/checkpoints/eo_spark_kafka",
           "--max-offsets", str(a.max_offsets)]
    ev2 = spark_kill_loop(cmd + ["--reset"], cmd, "/checkpoints/eo_spark_kafka", a.kills, "avro_silver.py")
    tools("tap", "--topics", "eo.spark.kafka", "--out", "/lab/.out/cmp/eo_spark_kafka.jsonl", "--idle-s", "6")
    rows = read_tap(OUT / "eo_spark_kafka.jsonl")
    keys = [key(r["v"]) for r in rows]
    res["kafka_sink"] = {"input_topic": "cell.kpi.avro (from the eo-flink stage)", "kills": ev2, "rows": len(rows),
                         "distinct_windows": len(set(keys)), "duplicate_rows": len(keys) - len(set(keys))}
    (RES / "exactly_once_spark.json").write_text(json.dumps(res, indent=2))
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "kills"} for k, v in res.items()}))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("stage", choices=["drain", "live", "eo-flink", "eo-spark"])
    p.add_argument("--hours", type=float, default=None)
    p.add_argument("--speed", type=float, default=60)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--max-offsets", type=int, default=200000)
    p.add_argument("--checkpoint", default="10 s")
    p.add_argument("--kill-at", type=float, nargs="+", default=[40, 80])
    p.add_argument("--down-s", type=float, default=3)
    p.add_argument("--kills", type=int, default=3)
    a = p.parse_args()
    defaults = {"drain": 12, "live": 2, "eo-flink": 4, "eo-spark": 6}
    a.hours = a.hours or defaults[a.stage]
    {"drain": stage_drain, "live": stage_live, "eo-flink": stage_eo_flink, "eo-spark": stage_eo_spark}[a.stage](a)


if __name__ == "__main__":
    main()
