"""Capture real UI screenshots of the running lab with Playwright (headless Chromium).

    python scripts/screenshots.py live    # while `make live` is running: Spark streaming UI, Kafka UI
    python scripts/screenshots.py after   # after a run: S3 console with the Iceberg table files

Credentials are read from .env and typed into the S3 console login form; never printed.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if line and not line.startswith("#") and "=" in line)
SPARK = f"http://127.0.0.1:{ENV.get('PORT_SPARK_UI', '24040')}"
KUI = f"http://127.0.0.1:{ENV.get('PORT_KAFKA_UI', '28080')}"
S3 = f"http://127.0.0.1:{ENV.get('PORT_S3_CONSOLE', '29001')}"


def shot(page, name: str, full: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    im = Image.open(path)
    if im.width > 1440:
        im.resize((1440, round(im.height * 1440 / im.width)), Image.LANCZOS).save(path, optimize=True)
    print("saved", path.relative_to(ROOT))


def live(page) -> None:
    page.goto(f"{SPARK}/StreamingQuery/")
    page.wait_for_timeout(1500)
    shot(page, "spark_streaming_queries")
    html = page.content()
    ids = dict((name, rid) for rid, name in re.findall(r'statistics/\?id=([0-9a-f-]+)"[^>]*>[^<]*</a>\s*</td>\s*<td[^>]*>\s*(\w+)', html))
    links = re.findall(r'(/StreamingQuery/statistics/\?id=[0-9a-f-]+)', html)
    names = re.findall(r'<td>\s*(bronze|silver|detect)\s*</td>', html)
    target = dict(zip(names, links)) if names and len(names) == len(links) else {}
    for name in ("silver", "detect"):
        url = target.get(name) or (links[0] if links else None)
        if not url:
            continue
        page.goto(SPARK + url)
        page.wait_for_timeout(2500)
        shot(page, f"spark_streaming_{name}", full=True)
    page.goto(f"{KUI}/ui/clusters/mf-realtime/all-topics/cell.kpi")
    page.wait_for_timeout(3000)
    shot(page, "kafka_ui_topic_overview")
    page.goto(f"{KUI}/ui/clusters/mf-realtime/all-topics/cell.kpi/messages")
    page.wait_for_timeout(3500)
    shot(page, "kafka_ui_messages")
    page.goto(f"{KUI}/ui/clusters/mf-realtime/all-topics")
    page.wait_for_timeout(2500)
    shot(page, "kafka_ui_topics")


def after(page) -> None:
    page.goto(f"{S3}/rustfs/console/auth/login/")
    page.fill("input[name=accessKey]", ENV["LAKE_S3_ACCESS_KEY"])
    page.fill("input[name=secretKey]", ENV["LAKE_S3_SECRET_KEY"])
    page.keyboard.press("Enter")
    page.wait_for_url("**/browser/**")
    page.wait_for_timeout(4500)
    page.goto(f"{S3}/rustfs/console/browser/?bucket=warehouse")
    page.wait_for_timeout(1500)
    for folder in ("rt/", "kpi_1m/", "data/"):
        page.get_by_text(folder, exact=True).first.click()
        page.wait_for_timeout(1500)
    shot(page, "s3_console_silver_partitions")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        {"live": live, "after": after}[sys.argv[1]](page)
        b.close()
