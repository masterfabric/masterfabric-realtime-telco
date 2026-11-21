"""Render flink/silver.sql and submit it to the lab Flink session cluster; optionally wait for it.

    python3 scripts/flink_job.py --name silver-drain --bounded --wait          # drain and finish
    python3 scripts/flink_job.py --name silver-live --guarantee exactly-once   # keep running

Runs on the host (talks to the JobManager REST API on 127.0.0.1:$PORT_FLINK_UI and uses docker exec
for the SQL client). Prints one JSON line with the job id and, with --wait, its duration.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REST = f"http://127.0.0.1:{os.environ.get('PORT_FLINK_UI', '28081')}"


def rest(path: str, method: str = "GET"):
    req = urllib.request.Request(REST + path, method=method)
    with urllib.request.urlopen(req, timeout=10) as r:
        body = r.read()
        return json.loads(body) if body else {}


def jobs_by_name(name: str) -> list[dict]:
    out = []
    for j in rest("/jobs/overview")["jobs"]:
        if j["name"] == name:
            out.append(j)
    return out


def submit(a: argparse.Namespace) -> str:
    sql = (ROOT / "flink/silver.sql").read_text()
    bounded = "'scan.bounded.mode' = 'latest-offset'," if a.bounded else ""
    for k, v in {"JOB_NAME": a.name, "IN_TOPIC": a.in_topic, "OUT_TOPIC": a.out_topic,
                 "GUARANTEE": a.guarantee, "CHECKPOINT": a.checkpoint, "BOUNDED": bounded}.items():
        sql = sql.replace("{{" + k + "}}", v)
    out = ROOT / ".out/flink"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{a.name}.sql").write_text(sql)
    before = {j["jid"] for j in jobs_by_name(a.name)}
    r = subprocess.run(["docker", "exec", "mf-rt-flink-jm", "/opt/flink/bin/sql-client.sh", "-f",
                        f"/lab/.out/flink/{a.name}.sql"], capture_output=True, text=True)
    if r.returncode != 0 or "ERROR" in r.stdout:
        raise SystemExit(r.stdout[-3000:] + r.stderr[-2000:])
    for _ in range(60):
        new = [j for j in jobs_by_name(a.name) if j["jid"] not in before]
        if new:
            return new[0]["jid"]
        time.sleep(0.5)
    raise SystemExit("job did not appear")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--name", required=True)
    p.add_argument("--in-topic", default="cell.kpi.avro")
    p.add_argument("--out-topic", default="kpi_1m.flink")
    p.add_argument("--guarantee", choices=["exactly-once", "at-least-once", "none"], default="at-least-once")
    p.add_argument("--checkpoint", default="10 s")
    p.add_argument("--bounded", action="store_true", help="stop at the offsets present at start-up")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--out", default=None)
    a = p.parse_args()
    t0 = time.time()
    jid = submit(a)
    res = {"engine": "flink", "job_id": jid, "name": a.name, "guarantee": a.guarantee}
    if a.wait:
        while True:
            j = rest(f"/jobs/{jid}")
            if j["state"] in ("FINISHED", "FAILED", "CANCELED"):
                break
            time.sleep(1)
        res.update(state=j["state"], job_duration_s=round(j["duration"] / 1000, 2),
                   submit_to_finish_s=round(time.time() - t0, 2),
                   records_in=sum(v["metrics"]["read-records"] for v in j["vertices"][:1]))
    print(json.dumps(res))
    if a.out:
        Path(a.out).write_text(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
