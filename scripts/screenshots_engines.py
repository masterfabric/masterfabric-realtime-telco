"""Real UI screenshots for the schema-registry and Flink additions (Playwright, headless Chromium).

Needs the stack with the "schema" and "flink" profiles. Starts an exactly-once Flink job on a fresh
live feed, kills the TaskManager once so the exceptions and checkpoint pages show a real recovery,
then captures the Flink UI, the Apicurio UI and Kafka UI's Avro-decoding message browser.

    .venv/bin/python scripts/screenshots_engines.py
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
FLINK, REG_UI, KUI = "http://127.0.0.1:28081", "http://127.0.0.1:28086", "http://127.0.0.1:28080"
SPARK = ["docker", "exec", "-w", "/lab", "mf-rt-spark"]


def shot(page, name: str, full: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    im = Image.open(path)
    if im.width > 1440:
        im.resize((1440, round(im.height * 1440 / im.width)), Image.LANCZOS).save(path, optimize=True)
    print("saved", path.relative_to(ROOT))


def rest(path: str) -> dict:
    with urllib.request.urlopen(FLINK + path, timeout=10) as r:
        return json.loads(r.read())


def main() -> None:
    subprocess.run(SPARK + ["python", "scripts/topic_tools.py", "recreate", "cell.kpi.avro:6", "kpi_1m.flink_eo:3"],
                   check=True, capture_output=True)
    r = subprocess.run(["python3", "scripts/flink_job.py", "--name", "silver-exactly-once", "--out-topic",
                        "kpi_1m.flink_eo", "--guarantee", "exactly-once", "--checkpoint", "10 s"],
                       check=True, capture_output=True, text=True)
    jid = json.loads(r.stdout.strip().splitlines()[-1])["job_id"]
    prod = threading.Thread(target=lambda: subprocess.run(
        SPARK + ["python", "-m", "producer.run", "--format", "avro", "--topic", "cell.kpi.avro", "--cells", "240",
                 "--hours", "3", "--speed", "60", "--warmup-min", "30", "--v2-from-minute", "40",
                 "--run-id", "shots-flink"], capture_output=True))
    prod.start()
    time.sleep(45)
    subprocess.run(["docker", "kill", "-s", "KILL", "mf-rt-flink-tm"], check=True, capture_output=True)
    time.sleep(3)
    subprocess.run(["docker", "start", "mf-rt-flink-tm"], check=True, capture_output=True)
    for _ in range(240):                                   # wait for the restart and a few checkpoints after it
        j = rest(f"/jobs/{jid}")
        ex = rest(f"/jobs/{jid}/exceptions").get("exceptionHistory", {}).get("entries", [])
        if ex and j["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in j["vertices"]):
            break
        time.sleep(1)
    time.sleep(35)
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 900})
        page.goto(f"{FLINK}/#/job/running/{jid}/overview")
        page.wait_for_timeout(4000)
        shot(page, "flink_job_graph")
        page.goto(f"{FLINK}/#/job/running/{jid}/exceptions")
        page.wait_for_timeout(3000)
        shot(page, "flink_exceptions_after_kill")
        page.goto(f"{FLINK}/#/job/running/{jid}/checkpoints")
        page.wait_for_timeout(3000)
        page.get_by_text("History", exact=True).first.click()
        page.wait_for_timeout(2000)
        shot(page, "flink_checkpoints")
        page.goto(f"{REG_UI}/explore/default/cell.kpi.avro-value")
        page.wait_for_timeout(3500)
        shot(page, "registry_versions")
        page.get_by_text("Rules", exact=True).first.click()
        page.wait_for_timeout(2000)
        shot(page, "registry_rules_backward")
        page.goto(f"{REG_UI}/explore/default/cell.kpi.avro-value/versions/2")
        page.wait_for_timeout(3000)
        page.get_by_text("Content", exact=True).first.click()
        page.wait_for_timeout(2000)
        shot(page, "registry_v2_content")
        page.goto(f"{KUI}/ui/clusters/mf-realtime/all-topics/cell.kpi.avro/messages?keySerde=String&valueSerde=SchemaRegistry&limit=50")
        page.wait_for_timeout(5000)
        shot(page, "kafka_ui_avro_messages")
        b.close()
    prod.join()
    urllib.request.urlopen(urllib.request.Request(f"{FLINK}/jobs/{jid}?mode=cancel", method="PATCH"), timeout=10)


if __name__ == "__main__":
    main()
