"""Drive the silver transformation by hand: one JSON file = one micro-batch, in arrival order.

Kafka decides batch boundaries by wall-clock triggers, so a test on Kafka cannot say "this report
arrives after that one, in a later batch". Here each call to `Replay.push(events)` writes one file and
runs exactly the batches it causes (including the no-data batch Spark runs when the watermark moves),
so the order of arrival, and therefore what is late, is fully controlled. The transformation itself is
`streaming.app.silver_frame`, the same function the live job runs.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from streaming.app import silver_frame
from streaming.schema import KPI_EVENT

REPLAY_SCHEMA = StructType(KPI_EVENT.fields + [StructField("kafka_ts", StringType())])


def local_spark(name: str = "replay") -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName(name)
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


class Replay:
    def __init__(self, spark: SparkSession, watermark: str = "2 minutes"):
        self.spark = spark
        self.dir = Path(tempfile.mkdtemp(prefix="replay-"))
        (self.dir / "in").mkdir()
        self.n = 0
        self.tag = None          # caller's label for the current push (e.g. the arrival time)
        self.rows: list[dict] = []
        self.progress: list[dict] = []
        src = (spark.readStream.schema(REPLAY_SCHEMA).option("maxFilesPerTrigger", 1)
               .json(str(self.dir / "in"))
               .withColumn("event_ts", F.to_timestamp("event_ts"))
               .withColumn("kafka_ts", F.to_timestamp("kafka_ts"))
               .withColumn("parse_ok", F.col("event_id").isNotNull() & F.col("event_ts").isNotNull()))

        def sink(df, batch_id):
            for r in df.drop("emitted_at").collect():
                d = r.asDict()
                d["emitted_with"] = self.tag
                self.rows.append(d)

        self.q = (silver_frame(src, watermark).writeStream.outputMode("append").foreachBatch(sink)
                  .option("checkpointLocation", str(self.dir / "chk")).start())

    def push(self, events: list[dict], tag=None) -> list[dict]:
        """Deliver one batch of events; returns the silver rows this delivery caused to be emitted."""
        self.tag = tag
        before = len(self.rows)
        tmp = self.dir / f".{self.n:06d}.json"
        tmp.write_text("".join(json.dumps(e) + "\n" for e in events))
        tmp.rename(self.dir / "in" / f"{self.n:06d}.json")      # atomic: the source never sees half a file
        self.n += 1
        self.q.processAllAvailable()
        for p in self.q.recentProgress:
            if not self.progress or p["batchId"] > self.progress[-1]["batchId"]:
                ops = p.get("stateOperators") or []
                self.progress.append({"batchId": p["batchId"], "rows": p["numInputRows"],
                                      "watermark": (p.get("eventTime") or {}).get("watermark"),
                                      "dropped": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
                                      "tag": tag})
        return self.rows[before:]

    @property
    def dropped(self) -> int:
        return sum(p["dropped"] for p in self.progress)

    @property
    def watermark(self) -> str | None:
        return self.progress[-1]["watermark"] if self.progress else None

    def stop(self) -> None:
        self.q.stop()
        shutil.rmtree(self.dir, ignore_errors=True)


def event(cell: str, ts: str, event_id: str | None = None, **kpi) -> dict:
    """A minimal valid report; only the fields the tests look at vary."""
    e = {"v": 1, "event_id": event_id or f"{cell}-{ts}", "event_ts": ts, "cell_id": cell, "site_id": "s1",
         "city": "Istanbul", "rat": "5G", "interval_s": 10, "active_users": 10.0, "prb_util_pct": 50.0,
         "dl_throughput_mbps": 100.0, "ul_sinr_db": 12.0, "latency_ms": 20.0, "rrc_attempts": 10,
         "rrc_success": 10, "erab_drops": 0, "availability_pct": 100.0, "kafka_ts": ts}
    e.update(kpi)
    return e
