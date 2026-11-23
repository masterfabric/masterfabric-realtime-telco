<p align="center"><img src="docs/hero.png" alt="MasterFabric Realtime Telco: Kafka, Spark Structured Streaming, Iceberg and anomaly detection, with measured results" width="100%"></p>

# MasterFabric · Realtime Telco

**Streaming network monitoring for a mobile operator, small enough to run on one laptop.** A simulated
radio network sends 10-second KPI reports from 240 cells into Kafka, delivered the way real mediation
chains deliver: late, out of order and sometimes twice. Spark Structured Streaming turns them into
Iceberg tables (bronze, silver, gold) and a per-cell stateful detector raises typed alerts: outage,
sleeping cell, congestion, interference and slow degradation. Every alert is scored against the
ground truth that produced the telemetry.

All numbers below come from runs on an Apple M4 Pro (24 GB) with Docker through Colima
(6 vCPU, 10 GB). They are read from [`docs/results/`](docs/results) and none are estimates.

| | Measured |
|---|---|
| Live run | 240 cells · 6 simulated hours at 30x · **520,931 events** delivered in 12 minutes |
| Detection (held-out seed) | **36/36 incidents**, 45 alerts, **2 false** · precision **0.956** · recall **1.0** · type accuracy **1.0** |
| Static thresholds, same windows | 334 alerts, **253 false** · precision **0.243** · 35/36 found |
| Latency, last report → alert | p50 **9.8 s**, p95 9.8 s (Kafka → watermark → silver → Iceberg stream → detector → MERGE + Kafka) |
| Data handling | 2,531 duplicates removed · 502 very late rows dropped by the watermark (kept in bronze) · 0 parse failures |
| Throughput (drain) | bronze **125,611 events/s** · silver 74,689–94,561 events/s · Spark `local[4]`, 6 partitions |
| Table maintenance | silver 357 → 6 files · per-cell aggregate **736.9 → 43.5 ms** |
| Schema registry (Avro) | BACKWARD rule: v2 accepted, incompatible v3 **rejected with 409** · Avro 103 B vs JSON 367 B per record |
| Flink 2.2 vs Spark 3.5, same job | drain 1.04M records **3.07 s vs 10.54 s** · live p50 **2.6 s vs 4.1 s** · CPU 12 % vs 83 % · windows **identical** |
| Exactly-once under kill -9 | Flink exactly-once sink **0** duplicates vs **4,080** at-least-once · Spark → Iceberg 521,044/521,044 offsets, **0** duplicates |

---

## Contents

[Why](#why-this-lab) · [Architecture](#architecture) · [The detector](#the-detector) ·
[Results](#results) · [Avro, Flink vs Spark, exactly-once](#schema-registry-flink-vs-spark-and-exactly-once) · [Late data and backpressure](#late-data-watermarks-and-backpressure) · [Screenshots](#what-it-looks-like) · [Quickstart](#quickstart) ·
[Walkthrough](#walkthrough) · [Enterprise mapping](#enterprise-mapping) · [Decisions](#design-decisions) ·
[Limitations](#limitations) · [Roadmap](#roadmap) · [Build timeline](docs/TIMELINE.md) · [License](#license)

## Why this lab

A network operations centre (NOC) in a large operator watches tens of thousands of cells. Three things
make its alarms hard to get right:

1. **Normal is different for every cell and every hour.** A residential cell at 21:00 and a business
   district cell at 04:00 have nothing in common. A static threshold that fits one is noise for the other.
2. **Telemetry arrives imperfectly.** PM files are collected in cycles, mediation re-sends, and links
   delay data. A streaming system has to decide what "this minute" means and when it is complete.
3. **An alarm has to be explainable and typed.** A field engineer needs "interference on IST-0007-1, SINR
   down 9 dB" and not just "anomaly score 0.93".

This repository builds that pipeline end to end with open components, measures it, and documents the
decisions in [ADRs](docs/adr). It is the streaming companion to
[masterfabric-lakehouse-lab](https://github.com/masterfabric/masterfabric-lakehouse-lab), which covers
the batch side.

## Architecture

<p align="center"><img src="docs/images/architecture.png" alt="Architecture diagram" width="100%"></p>
<p align="center"><sub><b>Figure 1.</b> Kafka feeds two independent Spark queries (bronze, silver). The detector reads silver back as an Iceberg stream, so the contract between aggregation and detection is a table that any engine can query or replay.</sub></p>

```mermaid
flowchart LR
  P["producer/run.py<br/>240 cells · 10 s reports<br/>late · very late · duplicates"] -->|"key = cell_id"| K[("Kafka<br/>cell.kpi · 6 partitions")]
  K --> B["bronze query<br/>explicit schema<br/>keep everything"]
  K --> S["silver query<br/>watermark 2 min<br/>dedupe · 1-min windows"]
  B -->|append| T1[("rt.kpi_events")]
  S -->|"append (final windows only)"| T2[("rt.kpi_1m")]
  T2 -->|Iceberg stream| D["detect query<br/>applyInPandasWithState"]
  D -->|MERGE| T3[("rt.alerts")]
  D -->|MERGE| T4[("rt.cell_health")]
  D -->|"key = cell_id"| KA[("Kafka<br/>cell.alerts · compacted")]
  T1 & T2 & T3 & T4 -.-> M["maintenance.py<br/>compaction · expiry"]
```

### What happens to one report

```mermaid
sequenceDiagram
  autonumber
  participant Cell as Cell IST-0007-1
  participant K as Kafka cell.kpi
  participant Br as bronze (5 s trigger)
  participant Si as silver (2 s trigger)
  participant De as detect (2 s trigger)
  participant Out as rt.alerts / cell.alerts
  Cell->>K: report 10:41:20 (arrives 35 s late)
  K->>Br: next micro-batch: parsed, appended with partition/offset
  K->>Si: dedupe on event_id, added to window 10:41
  Note over Si: window 10:41 stays open until<br/>watermark = max event time − 2 min passes 10:42
  Si->>De: window committed to rt.kpi_1m, read as an Iceberg stream
  De->>De: EWMA z-scores, CUSUM, rules · per-cell state in RocksDB
  De->>Out: OPEN interference (evidence JSON), MERGE + Kafka
```

The three queries have separate checkpoints. Bronze never waits for aggregation, and silver can be
rebuilt from Kafka (or from bronze) without touching raw capture. See
[ADR 0004](docs/adr/0004-iceberg-between-stages.md).

## The detector

<p align="center"><img src="docs/images/detector_flow.png" alt="Detector decision flow" width="100%"></p>
<p align="center"><sub><b>Figure 2.</b> One finalised minute for one cell passes through hard rules, adaptive z-scores and a Bernoulli CUSUM. The most severe condition wins, and the baseline learns only from calm windows.</sub></p>

The detector ([`streaming/detector.py`](streaming/detector.py)) keeps a few floats per cell: EWMA mean
and variance of PRB, latency, SINR and RRC failure rate, a CUSUM statistic and the open alert. That
fits in Spark's `applyInPandasWithState` (240 state rows, 46 MiB in the live run) and runs unchanged
offline with pandas ([`streaming/offline.py`](streaming/offline.py)).

| Incident | Signature in the data | Rule | Windows needed |
|---|---|---|---|
| Outage | availability 0 | availability < 50% | 1 |
| Sleeping cell | reports itself up, almost all RRC setups fail | ≥ 20 attempts and > 50% fail | 1 |
| Congestion | PRB pinned at the ceiling, latency ×5 | PRB ≥ 95% **and** latency z ≥ 4 | 2 |
| Interference | UL SINR down ~9 dB, drops up | SINR z ≤ −4 against the cell's own baseline | 2 |
| Degradation | RRC failure rate drifts from 0.5% to ~9.5% over 30–60 min | Bernoulli CUSUM ≥ 14 nats | 1 |
| Flash crowd (benign) | a stadium site at 1.5× load | must **not** alert | n/a |

```mermaid
stateDiagram-v2
  [*] --> Learning: first window
  Learning --> Normal: 30 calm windows
  Learning --> Open: outage / sleeping cell (hard rules work from window 1)
  Normal --> Open: condition holds for the required windows
  Open --> Open: more severe condition → close old, open new
  Open --> Normal: 3 consecutive healthy windows → CLOSED
  note right of Open: baseline frozen while open
```

**Tuning and judging are separated.** Thresholds were chosen on a development seed (7) with the
offline replay, and the live pipeline was then scored on seed 2026, which was not used for tuning. Two
findings from that process are in the git history:

- A Gaussian z-score on the RRC failure rate produced **88 false alerts** on the development seed.
  Failures are small counts (about 0.5 per cell-minute), so the normal approximation is wrong. A
  Bernoulli log-likelihood CUSUM brought that to 1 at the same recall.
- The first live run found every incident but left **10 cells flagged as degraded** at the end: the
  uncapped CUSUM had climbed into the hundreds and needed hours to drain. Capping it at 1.5× the
  threshold fixed this (0 cells stuck), and detection did not change
  ([`live_run1_uncapped_cusum.json`](docs/results/live_run1_uncapped_cusum.json)).

<p align="center"><img src="docs/images/cusum_sweep.png" alt="CUSUM threshold sweep" width="70%"></p>
<p align="center"><sub><b>Figure 3.</b> Development seed only: false alerts against the median degradation delay for each CUSUM threshold. Recall is 1.0 at every setting; h = 14 was chosen.</sub></p>

## Results

### Detection on the held-out seed

<p align="center"><img src="docs/images/detection_traces.png" alt="KPI traces with ground truth and alerts" width="100%"></p>
<p align="center"><sub><b>Figure 4.</b> Real silver windows from the live run around one incident of each kind. Shaded: ground truth. Red dashed: alert opened. Green dashed: alert closed. The flash crowd (bottom right) raised no alert.</sub></p>

| Incident type | Incidents | Detected | Type correct | Median delay | Max delay |
|---|---:|---:|---:|---:|---:|
| Outage | 6 | 6 | 6 | 1.0 min | 1.0 min |
| Sleeping cell | 4 | 4 | 4 | 1.0 min | 1.0 min |
| Congestion | 5 | 5 | 5 | 2.0 min | 2.0 min |
| Interference | 7 | 7 | 7 | 3.0 min | 5.0 min |
| Degradation | 14 | 14 | 14 | 12.5 min | 24.0 min |
| **All** | **36** | **36** | **36** | **3.0 min** | p90 15.0 min |

Delays are in simulated minutes, from incident start to the end of the first alerting window. A
degradation starts invisibly small, so it takes longest to detect by design.

<p align="center"><img src="docs/images/adaptive_vs_static.png" alt="Adaptive versus static thresholds" width="100%"></p>
<p align="center"><sub><b>Figure 5.</b> The same 85,680 silver windows through the adaptive detector and a static rule set (PRB > 85%, latency > 30 ms, SINR < 6 dB, RRC success < 97%). The static rules find almost as many incidents but bury them under 253 false alerts, mostly busy-but-healthy cells and cells whose normal SINR is low.</sub></p>

### Latency

<p align="center"><img src="docs/images/latency_cdf.png" alt="Latency distributions" width="85%"></p>
<p align="center"><sub><b>Figure 6.</b> Wall-clock latency from the Kafka timestamp of a window's last report to the silver commit (85,680 windows) and to the alert (45 alerts).</sub></p>

| Hop | p50 | p95 | max |
|---|---:|---:|---:|
| Kafka → bronze row | 2.4 s | 4.7 s | 4.8 s |
| last report → silver window committed | 7.7 s | 7.7 s | 8.1 s |
| last report → alert in `rt.alerts` and `cell.alerts` | **9.8 s** | 9.8 s | 9.8 s |

Most of the silver latency is the watermark: a window is final only when event time has moved two
simulated minutes past it (4 s of wall time at 30×), plus a 2-second trigger. Both are settings, not limits.
In real time (1×), the watermark wait would be the full 2 minutes, which is the price of correctness
for late data ([ADR 0003](docs/adr/0003-event-time-watermarks.md)).

### Throughput and state

<p align="center"><img src="docs/images/bench_throughput.png" alt="Drain throughput" width="85%"></p>
<p align="center"><sub><b>Figure 7.</b> Draining a pre-filled topic of 1,042,065 events with availableNow triggers. Each configuration reads the whole topic, so "bronze + silver" processes every event twice.</sub></p>

| Configuration | Events | Batches | Seconds | Events/s | Largest batch |
|---|---:|---:|---:|---:|---:|
| bronze · maxOffsetsPerTrigger 100k | 1,042,065 | 11 | 8.30 | **125,611** | 2.3 s |
| silver · 100k | 1,042,065 | 11 | 13.95 | 74,689 | 3.0 s |
| silver · 500k | 1,042,065 | 3 | 11.02 | 94,561 | 4.8 s |
| bronze + silver · 100k | 1,042,065 | 22 | 15.03 | 69,314 | 3.2 s |

The live run needed about 724 events/s, so the laptop has roughly 100× headroom. Larger batches help
silver (fewer state store commits) at the cost of a longer worst-case batch. The Python producer
itself reached 88,540 events/s.

<p align="center"><img src="docs/images/stream_batches.png" alt="Batch durations and state" width="100%"></p>
<p align="center"><sub><b>Figure 8.</b> Live run, every micro-batch. Left: batch durations (median 0.73 s bronze, 0.89 s silver, 0.85 s detect) stay under the 2-second trigger, so no backlog builds. Right: silver state rows stay flat (bounded by the watermark) while RocksDB memory kept growing to 94 MiB over 12 minutes; see Limitations.</sub></p>

### Iceberg tables after a streaming run

Streaming commits every few seconds leave many small files and snapshots. Numbers before maintenance
come from the evaluation right after the run, and numbers after come from
[`maintenance_live.json`](docs/results/maintenance_live.json).

| Table | Data files | Snapshots | After maintenance |
|---|---:|---:|---|
| `rt.kpi_events` (bronze) | 167 | 146 | 6 files · 1 snapshot |
| `rt.kpi_1m` (silver) | 357 (15.3 KiB avg) | 363 | 6 files (354.6 KiB avg) · 1 snapshot |
| `rt.alerts` | 83 | 68 | 5 files · 1 snapshot |
| `rt.cell_health` | 32 | 68 | 1 file · 1 snapshot |

A per-cell aggregate over silver went from **736.9 ms to 43.5 ms** (median of 5) after compaction.

### Reconciliation

| Check | Count |
|---|---:|
| Producer reports / delivered (incl. re-sends) | 518,400 / 520,931 |
| Bronze rows / unique `event_id` | 520,931 / 518,400 |
| Duplicates removed in silver | 2,531 (= injected re-sends) |
| Silver windows / reports inside them | 85,680 / 513,579 |
| Not in silver | 4,821 = 502 very late rows dropped by the watermark + the last ~2 simulated minutes, which never close once input stops |

## What it looks like

<p align="center"><img src="docs/screenshots/spark_streaming_queries.png" alt="Spark Structured Streaming queries" width="100%"></p>
<p align="center"><sub><b>Figure 9.</b> Spark UI during the live run: the three queries running side by side, about 724 input rows/s each for bronze and silver and 120 windows/s for the detector.</sub></p>

<p align="center"><img src="docs/screenshots/spark_streaming_silver.png" alt="Silver query statistics" width="70%"></p>
<p align="center"><sub><b>Figure 10.</b> Silver query statistics: steady input rate, batch durations around 1 s, flat state rows, and a few rows per batch dropped by the watermark (the very late reports). The "global watermark gap" is large because event time is simulated (16 Nov) while the wall clock is real.</sub></p>

<p align="center"><img src="docs/screenshots/kafka_ui_messages.png" alt="Kafka UI messages" width="100%"></p>
<p align="center"><sub><b>Figure 11.</b> kafbat UI on <code>cell.kpi</code>: JSON payloads keyed by cell ID across six partitions.</sub></p>

<p align="center"><img src="docs/screenshots/kafka_ui_topic_overview.png" alt="Kafka topic overview" width="100%"></p>
<p align="center"><sub><b>Figure 12.</b> Partition sizes at the same moment ranged from 8,334 to 11,518 messages: keying by cell keeps per-cell order but follows the hash of the keys (<a href="docs/adr/0001-kafka-kraft-backbone.md">ADR 0001</a>).</sub></p>

<p align="center"><img src="docs/screenshots/s3_console_silver_partitions.png" alt="RustFS console with silver partitions" width="100%"></p>
<p align="center"><sub><b>Figure 13.</b> The silver table in object storage: Iceberg hidden partitioning by <code>hours(window_start)</code>, one folder per simulated hour.</sub></p>

## Schema registry, Flink vs Spark and exactly-once

Three questions the first version answered only in ADRs: what a schema registry actually enforces,
what Flink would buy over Spark on this exact job, and whether the exactly-once claims survive a
`kill -9`. Everything runs in the same Compose project behind two profiles (`schema`, `flink`).

<p align="center"><img src="docs/images/engines_flow.png" alt="Avro producer, Apicurio registry, one Kafka topic read by Spark and by Flink, outputs compared" width="100%"></p>
<p align="center"><sub><b>Figure 14.</b> The Avro path: the producer registers its schema and writes the Confluent wire format; Spark and Flink run the same silver aggregation on that topic and their outputs are compared window by window.</sub></p>

### Avro and the schema registry

[Apicurio Registry 3.3](https://www.apicur.io/registry/) (Apache-2.0) serves the Confluent-compatible
API, so the producer, Spark and Flink would work unchanged against Confluent Schema Registry or Karapace
([ADR 0008](docs/adr/0008-avro-schema-registry.md)). Subject `cell.kpi.avro-value` has a BACKWARD rule.
From [`schema_registry.json`](docs/results/schema_registry.json):

| Step | Result |
|---|---|
| Register v1 (`schemas/cell_kpi_v1.avsc`, same fields as the JSON contract) | id assigned, 49.5 ms |
| Check / register v2 (adds optional `energy_kwh`, default null) | compatible · registered |
| Check / register v3 (adds required `vendor`, no default) | incompatible · **HTTP 409**, `vendor at /fields/18` |
| Re-register v1 | same id returned |
| v1 record read with the v2 reader schema | `energy_kwh = null` |
| Size | 103 B Avro (5 B header) vs 367 B JSON per record · 120.5 MB vs 403.1 MB over 1,042,006 records, before Kafka compression |

The producer switches from v1 to v2 in the middle of a run (`--v2-from-minute`); both engines read
the mixed topic. Spark needs one `from_avro` per schema id; the first version used a union of one
branch per id and read the Kafka source twice per batch, so the final version uses one `CASE`.

<p align="center"><img src="docs/screenshots/registry_versions.png" alt="Apicurio UI with two versions of cell.kpi.avro-value" width="49%"> <img src="docs/screenshots/registry_rules_backward.png" alt="Apicurio UI with the Backward compatibility rule" width="49%"></p>
<p align="center"><sub><b>Figure 15.</b> Apicurio UI: the subject holds v1 and v2 only (v3 was refused), and the artifact rule is Backward.</sub></p>

<p align="center"><img src="docs/screenshots/kafka_ui_avro_messages.png" alt="Kafka UI showing decoded Avro messages from cell.kpi.avro" width="100%"></p>
<p align="center"><sub><b>Figure 16.</b> kafbat UI decodes <code>cell.kpi.avro</code> through the registry: v2 records with <code>energy_kwh</code>, keyed by cell.</sub></p>

### Flink vs Spark on the same job

`streaming/avro_silver.py` (Spark 3.5.5, `local[6]`) and `flink/silver.sql` (Flink 2.2.1, parallelism
6, one 3 GB TaskManager) both apply a 2-minute watermark, drop duplicate `event_id`s and write one row
per cell and minute to Kafka ([ADR 0009](docs/adr/0009-flink-vs-spark-measured.md)). Sources:
[`engine_drain.json`](docs/results/engine_drain.json), [`engine_live.json`](docs/results/engine_live.json).

| | Spark | Flink |
|---|---:|---:|
| Drain 1,042,006 records (12 simulated hours), median of 3 | 10.54 s · 98,862 rec/s | **3.07 s · 339,416 rec/s** |
| Same including JVM start / job submission | 12.33 s | 7.79 s |
| Live, 173,701 records at 1,449 events/s: latency p50 · p95 · p99 | 4.06 · 4.35 · 4.47 s | **2.60 · 2.71 · 2.74 s** |
| Flink with the exactly-once Kafka sink (checkpoint 10 s) | | 7.72 · 11.75 · 11.88 s |
| Average CPU during the live feed | 83 % (Spark container) | 12 % (TaskManager, 2 jobs) |
| Windows equal to the other engine | 28,080 / 28,080 live · 172,080 / 172,080 drain | |

Latency is measured from the Kafka timestamp of a window's last on-time input to the moment a
`read_committed` consumer saw the output row. The 2-minute watermark alone is 2 s of wall time at 60x;
Flink adds 0.6 s to that, Spark about 2 s, consistent with one
1-second trigger to advance the watermark and the next to emit. With the exactly-once sink, rows wait for the checkpoint that commits the transaction. Both
engines dropped the same 188 very late reports and match a replay of the input with a global 2-minute
watermark on every window.

<p align="center"><img src="docs/images/engine_comparison.png" alt="Drain throughput, live latency percentiles and CPU for Spark and Flink" width="100%"></p>
<p align="center"><sub><b>Figure 17.</b> Left: drain throughput. Middle: live latency percentiles; the dashed line is the watermark floor. Right: CPU and peak memory during the live feed.</sub></p>

<p align="center"><img src="docs/screenshots/flink_job_graph.png" alt="Flink job graph: source, window dedupe, window aggregate, Kafka writer and committer" width="100%"></p>
<p align="center"><sub><b>Figure 18.</b> Flink UI: the planner turned the SQL into source → window dedupe + local window aggregate → global window aggregate → Kafka writer and committer, parallelism 6.</sub></p>

### Exactly-once under kill -9

`make eo-flink` and `make eo-spark` kill processes with SIGKILL in the middle of a run and count what
comes out ([ADR 0010](docs/adr/0010-exactly-once-kill-tests.md); [`exactly_once_flink.json`](docs/results/exactly_once_flink.json),
[`exactly_once_spark.json`](docs/results/exactly_once_spark.json)).

| Sink | Kills | Output | Duplicates |
|---|---|---|---:|
| Flink exactly-once, read with `read_committed` | 2 × TaskManager | 56,880 windows | **0** |
| Same topic, read with `read_uncommitted` | | 58,320 rows | 1,440 |
| Flink at-least-once | 2 × TaskManager | 60,960 rows | **4,080** |
| Spark → Iceberg bronze | 3 × driver | 521,044 rows = 521,044 offsets, 0 gaps | **0** |
| Spark → Kafka (at-least-once by design) | 3 × driver | 56,880 windows | 0 observed |

Flink detected the lost TaskManager after 15.7 s with default heartbeat settings and had all tasks
running again 18.8–18.9 s after the kill. A restarted Spark driver committed its next batch 4.4–4.7 s
after launch.

<p align="center"><img src="docs/images/exactly_once.png" alt="Duplicate rows after kill -9 for each sink" width="100%"></p>
<p align="center"><sub><b>Figure 19.</b> Duplicate output rows after the kills. Exactly-once into Kafka holds only for <code>read_committed</code> readers; a default reader sees the aborted copies.</sub></p>

<p align="center"><img src="docs/screenshots/flink_exceptions_after_kill.png" alt="Flink exceptions tab: TaskManager no longer reachable" width="49%"> <img src="docs/screenshots/flink_checkpoints.png" alt="Flink checkpoint history with one failed checkpoint during the kill" width="49%"></p>
<p align="center"><sub><b>Figure 20.</b> The same recovery in the Flink UI: "TaskManager … is no longer reachable", and checkpoint 5 failed during the outage while the following ones complete again.</sub></p>

## Late data, watermarks and backpressure

Two questions the live run cannot answer cleanly: what exactly happens to a report that arrives
late, and what happens to latency when input outruns the job.

### Late and out-of-order reports, one micro-batch at a time

`streaming/replay.py` drives the real silver code (`silver_frame`, the function the live job runs)
from files instead of Kafka: one file is one micro-batch, so the test decides which report arrives
after which. `make test-spark` runs seven cases ([`tests/spark/test_watermark.py`](tests/spark/test_watermark.py)):

| Case | What arrives | Silver |
|---|---|---|
| Out of order | 05:00:50, 05:00:10, 05:00:30 in one batch | one window, 3 reports, correct average and max |
| Not yet final | window 05:00, then the stream reaches 05:02:59 | nothing emitted until the watermark reaches 05:01:00 |
| Late, inside the watermark | 05:00:40 after the stream reached 05:01:30 | counted (2 reports), 0 dropped |
| Late, behind the watermark | 05:00:50 after the window was emitted | dropped, counted in `numRowsDroppedByWatermark`; the window is never re-emitted |
| Retry, same `event_id` | the same report in two batches | counted once |
| Retry after the window closed | the same report after emission | the emitted window stays at 1 report |
| Lagging clock | one cell 3 minutes behind the others | every report of that cell after the first batch is dropped |

The last row is the one to remember: there is **one watermark per query, not per cell**, so a single
source with a skewed clock is silently discarded while everything else looks healthy.

### How long to wait: a watermark sweep

`make watermark-sweep` records one delivery with the producer (seed 2026, 240 cells, 2 simulated
hours, 173,701 deliveries of 172,800 reports, 901 of them re-sends) together with each report's simulated arrival time,
cuts it into 30-second arrival batches and replays it through silver once per watermark delay
([`watermark_sweep.json`](docs/results/watermark_sweep.json)).

<p align="center"><img src="docs/images/watermark_tradeoff.png" alt="Arrival lag histogram, reports lost and emit delay per watermark delay" width="100%"></p>
<p align="center"><sub><b>Figure 21.</b> Left: how late reports arrive (96.4 % within 2 s, 3.5 % at 2 to 60 s, 192 at 5 to 10 minutes). Middle: distinct reports missing from emitted windows, and the share of the 28,800 windows that are incomplete. Right: simulated time from a window's end to the batch that emitted it; at 1x speed these are real minutes.</sub></p>

| Watermark | Reports lost | Windows incomplete | Window end → emitted |
|---|---:|---:|---:|
| 10 s | 2,999 | 9.95 % | 0.5 min |
| 30 s | 1,087 | 3.70 % | 1.0 min |
| 1 min | 190 | 0.66 % | 1.5 min |
| **2 min (the live setting)** | **188** | **0.65 %** | **2.5 min** |
| 5 min | 178 | 0.62 % | 5.5 min |
| 11 min | 0 | 0 % | 11.5 min |

From 1 minute on, all ordinary lateness is absorbed; what is still lost is the 5-to-10-minute tail,
which only an 11-minute watermark keeps, at 4.6 times the delay of the 2-minute setting. The emit
delay is the watermark plus one 30-second batch, because Spark applies a new watermark in the batch
after the one that moved it. All 28,800 windows were emitted at every setting: a lost report makes a
window incomplete, it does not remove it. [ADR 0003](docs/adr/0003-event-time-watermarks.md) has the
reasoning for keeping 2 minutes.

### Backpressure: what happens when input outruns the job

Structured Streaming has no automatic backpressure for Kafka. `maxOffsetsPerTrigger` caps what one
micro-batch reads and everything else waits in Kafka, so when input is faster than the cap the
backlog, and with it latency, grows for as long as the overload lasts. `make backpressure` runs the
silver query alone with a cap of 2,000 records per 2-second trigger (1,000 records/s) and feeds
one simulated hour (86,845 records) at 0.5x, 1x, 1.5x and 1.9x that rate, then 1.9x without a cap
([`backpressure.json`](docs/results/backpressure.json)). Latency is from the Kafka timestamp of a
window's last report to the silver row.

<p align="center"><img src="docs/images/backpressure.png" alt="Silver latency over time and latency percentiles per input rate" width="100%"></p>
<p align="center"><sub><b>Figure 22.</b> Left: median latency of each emitted batch of windows over the run. Below the cap latency is flat; above it, it climbs in a straight line until the producer stops and the backlog drains. Right: p50 and max per run.</sub></p>

| Input | vs cap | Latency p50 | p95 | max | Last window emitted | Producer finished |
|---|---:|---:|---:|---:|---:|---:|
| 484 events/s | 0.5x | 8.6 s | 9.6 s | 10.1 s | 180.1 s | 179.5 s |
| 968 events/s | 1.0x | 6.4 s | 7.2 s | 7.9 s | 90.9 s | 89.8 s |
| 1,451 events/s | 1.5x | 18.4 s | 30.3 s | **31.6 s** | 87.3 s | 59.8 s |
| 1,935 events/s | 1.9x | 25.4 s | 43.9 s | **45.3 s** | 87.1 s | 44.9 s |
| 1,935 events/s | no cap | 4.6 s | 5.6 s | 5.7 s | 46.4 s | 44.9 s |

- **Overload latency is arithmetic.** Latency grows by (input − cap) / cap seconds per second:
  0.45 s/s at 1.5x and 0.94 s/s at 1.9x. Over the 60 s and 45 s of overload that predicts about 27 s
  and 42 s on top of the 3 to 4 s floor, which is what was measured (31.6 s and 45.3 s).
- **The cap sets the finish time, not the input.** Both overloaded runs emitted their last window at
  87 s, 86,845 records at 1,000 per second, however fast they were produced.
- **The slow run is not the fast one.** At 0.5x the floor is higher than at 1x (8.6 s against 6.4 s)
  because the 2-minute watermark is 6 s of wall time at 20x and 3 s at 40x; the waiting is event
  time, not load.
- **The cap was the bottleneck, not Spark.** Without it the same 1,935 events/s ran at p50 4.6 s with
  a median batch of 0.6 s. A cap protects a shared cluster from a replay or a catch-up; set it from
  measured batch time, and alert on consumer lag, because latency itself only shows the problem
  after the backlog exists.

## Quickstart

Requirements: Docker with Compose (Docker Desktop, Colima or OrbStack) and about 8 GB of memory for the
VM. For screenshots and charts you also need [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/masterfabric/masterfabric-realtime-telco.git
cd masterfabric-realtime-telco
make up          # Kafka, Kafka UI, RustFS, Iceberg REST catalog, Spark (builds the Spark image once)
make test-unit   # 25 producer/detector/Avro tests inside the Spark container
make live        # fresh topic, streaming app, 6 simulated hours at 30x (about 13 minutes)
make evaluate    # reconcile, score and time it into docs/results/live_live.json
```

| Service | URL (all bound to 127.0.0.1) |
|---|---|
| Kafka UI | http://127.0.0.1:28080 |
| Spark UI (streaming app, while running) | http://127.0.0.1:24040/StreamingQuery/ |
| Spark History | http://127.0.0.1:28090 |
| RustFS console | http://127.0.0.1:29001/rustfs/console/ (credentials in your local `.env`) |
| Kafka from the host | `127.0.0.1:29092` |

The Avro, Flink and exactly-once additions:

```bash
make flink-up        # + Apicurio registry and UI, Flink 2.2 JobManager and TaskManager
make registry-demo   # v1, BACKWARD rule, v2 accepted, v3 rejected
make compare-drain   # Spark vs Flink throughput, 3 runs each (about 2 minutes)
make compare-live    # both engines on a live feed (about 4 minutes)
make eo-flink        # TaskManager killed twice (about 6 minutes)
make eo-spark        # Spark driver killed 3 times per sink (about 2 minutes)
```

| Service | URL |
|---|---|
| Schema registry (Confluent-compatible API) | http://127.0.0.1:28085/apis/ccompat/v7 |
| Apicurio Registry UI | http://127.0.0.1:28086 |
| Flink UI | http://127.0.0.1:28081 |

`make down` stops everything, including both profiles, and keeps the volumes.

## Walkthrough

| Step | Command | What to look at |
|---|---|---|
| 1. Tune offline | `make offline SEED=7` | Same detector over a file dump with pandas; CUSUM sweep in `docs/results/offline_dev_seed7.json` |
| 2. Run live | `make live` | Spark UI Structured Streaming tab; Kafka UI topic `cell.kpi`, then `cell.alerts` |
| 3. Score it | `make evaluate` | precision/recall per incident type, latency per hop, reconciliation |
| 4. Maintain | `make maintain` | small files and snapshots before and after, probe query speed-up |
| 5. Benchmark | `make bench` | drain throughput in four configurations |
| 6. Visuals | `make tools assets` and `make screenshots MODE=live` while step 2 runs | everything under `docs/images` and `docs/screenshots` |

Producer knobs: `CELLS`, `HOURS`, `SPEED`, `--late-rate`, `--very-late-rate`, `--duplicate-rate`,
`--incidents-per-cell-hour`. Streaming knobs: `--watermark-min`, `--trigger-*`, `--max-offsets`.

```text
producer/    network model, KPI telemetry, Kafka producer with imperfect delivery + ground truth
streaming/   app.py (3 queries) · detector.py · schema.py · tables.py · evaluate.py · offline.py · maintenance.py
scripts/     live_run.sh · bench.sh · screenshots, diagrams, charts, hero
docker/      compose.yaml · Spark image (Iceberg + Kafka connector) · topic and bucket init
docs/        adr/ · results/ · images/ · screenshots/ · TIMELINE.md
tests/unit/  producer and detector tests
```

## Enterprise mapping

<p align="center"><img src="docs/images/cloudera_mapping.png" alt="Mapping to Cloudera CDP" width="100%"></p>
<p align="center"><sub><b>Figure 23.</b> Each lab component and its counterpart in a Cloudera CDP streaming estate.</sub></p>

The patterns carry over unchanged: keyed topics, event-time windows with a watermark, dedupe within the
watermark, independent queries per sink, an Iceberg table as the hand-off between streaming stages,
idempotent `MERGE` in `foreachBatch`, and scheduled compaction. What changes at scale is operational:
RF 3 with Kerberos/TLS, Ranger policies, checkpoints on HDFS/Ozone, a schema registry, and executors
on YARN or Kubernetes instead of `local[4]`.

## Design decisions

| ADR | Decision |
|---|---|
| [0001](docs/adr/0001-kafka-kraft-backbone.md) | Kafka in KRaft mode as the event backbone, keyed by cell |
| [0002](docs/adr/0002-spark-structured-streaming.md) | Spark Structured Streaming rather than Flink for this lab |
| [0003](docs/adr/0003-event-time-watermarks.md) | Event time, a 2-minute watermark and dedupe within the watermark |
| [0004](docs/adr/0004-iceberg-between-stages.md) | Iceberg tables, not topics, as the hand-off between streaming stages |
| [0005](docs/adr/0005-adaptive-detector.md) | Per-cell adaptive baselines and a Bernoulli CUSUM instead of a trained model |
| [0006](docs/adr/0006-exactly-once-effects.md) | Exactly-once effects: checkpoints, idempotent MERGE, keyed at-least-once alerts |
| [0007](docs/adr/0007-payload-contract.md) | JSON with an explicit schema contract; Avro/Protobuf + registry in production |
| [0008](docs/adr/0008-avro-schema-registry.md) | An Avro topic with a schema registry (Apicurio, BACKWARD rule) next to the JSON contract |
| [0009](docs/adr/0009-flink-vs-spark-measured.md) | Flink vs Spark Structured Streaming, measured on the same job |
| [0010](docs/adr/0010-exactly-once-kill-tests.md) | Exactly-once claims are tested with kill -9, not asserted |

## Limitations

These are deliberate simplifications or measured weaknesses, listed so the numbers above are read correctly.

- **Synthetic network.** Incidents are clean, labelled and single-cell. Real networks have correlated
  failures (a site power cut, a transport link, a core outage), maintenance windows and counter resets.
  The detector's precision and recall here are an upper bound.
- **Alert flapping during long degradations.** The 14 degradation incidents produced 23 alert openings:
  the capped CUSUM can dip under the threshold for three windows mid-incident and re-open. All count as
  true alerts, but a NOC would see them as noise. Hysteresis (close only below half the threshold) is
  the next change.
- **Simulated clock.** Event time runs 30× faster than wall time, so watermark waits and detection delays
  in wall seconds are 30× shorter than they would be in production. Delays are reported in simulated
  minutes where it matters. At 120× (the smoke test) a 5-second trigger spanned 10 simulated minutes and
  even "very late" reports made it into windows, which is why the live run uses 30× and 2-second triggers.
- **RocksDB memory grew** to 94 MiB over the 12-minute run while state rows stayed flat. It was not
  investigated further. A long-running job should set
  `spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage` and watch it.
- **Single broker, PLAINTEXT, local checkpoints.** Kafka has replication factor 1 and no authentication.
  Checkpoints are on a Docker volume (79 MiB after the live run), and the catalog uses `ResolvingFileIO`
  so the Iceberg streaming source can keep its offsets there. Production needs RF 3, TLS/SASL and
  checkpoints on HDFS or S3.
- **Spark, not Flink, for the main pipeline.** Micro-batches give second-level latency, fine for NOC
  alarms ([ADR 0002](docs/adr/0002-spark-structured-streaming.md)). The measured comparison
  ([ADR 0009](docs/adr/0009-flink-vs-spark-measured.md)) shows Flink faster and much cheaper in CPU on
  the Kafka-to-Kafka silver step; the detector and Iceberg pipeline were not ported.
- **The engine comparison is one laptop and one live run.** Both engines shared 6 vCPUs while live, the
  Spark container also hosts an idle Thrift server, history server and Jupyter (0.3 % CPU, 768 MiB),
  and nothing was tuned (trigger, state backends, Kafka sink settings). On a backlog neither engine
  dropped late data, because the watermark only moves per micro-batch or every 200 ms; lateness is only
  exercised in the live run. Spark's `availableNow` run leaves the last 3 open minutes (720 windows)
  unflushed, a bounded Flink job closes them.
- **Spark reads schema ids once.** Open-source Spark has no registry lookup; `avro_silver.py` reads the
  subject's versions at start-up, and a version registered later is skipped until a restart.
- **Spark's Kafka sink showed 0 duplicates in 3 kills, which proves nothing.** It is at-least-once by
  design; the kills simply missed the short window between the sink flush and the commit log.
- **The registry answers before it is ready.** After a restart Apicurio returned an empty subject list
  for up to about 10 seconds while it replayed its Kafka journal. Clients should wait for `/health/ready`.
- **Exactly-once into Kafka needs `read_committed` readers** and makes output wait for the checkpoint
  (p50 7.7 s instead of 2.6 s with 10-second checkpoints).
- **Late-data tests run on a file source, not Kafka.** The replay harness controls batch boundaries,
  which Kafka triggers cannot; the transformation is the same function, but the sweep's emit delays
  are simulated time quantised by its 30-second batches, and it covers one seed and one delivery
  profile (3 % late, 0.1 % very late).
- **Backpressure runs are short and single.** One run per rate, one simulated hour each, silver query
  only, on a laptop with Kafka and Spark sharing 6 vCPUs. The no-cap run shows the job had headroom
  at 1,935 events/s; the cap was set low on purpose to make overload visible. Consumer lag was not
  sampled from Kafka directly; latency stands in for it.
- **Object storage is RustFS, not MinIO.** The MinIO images could not be pulled from this machine
  (Docker Hub returned "access denied", quay.io 401). RustFS speaks the same S3 API.
- **The tail of a run is not scored.** Windows in the last two simulated minutes never close once input
  stops, so incidents starting there are excluded.
- **The Spark Thrift server** started by the base image holds port 4040. The streaming UI is on 4041
  (host 24040).

## Roadmap

- Alert hysteresis and suppression of child alerts under a site-level parent (correlated failures).
- A learned component next to the rules: an isolation forest on silver windows, trained and registered
  through the MLOps lab, judged with the same scorer.
- A dead-letter topic for payloads that fail the Avro or JSON contract, and Protobuf next to Avro.
- Port the detector to Flink (keyed process function) to compare state size and alert latency, not only the silver step.
- Multi-broker Kafka with TLS in Compose, and the detector's state TTL for decommissioned cells.

## License

Copyright © 2026 MasterFabric. Licensed under the **GNU Affero General Public License v3.0**; see
[LICENSE](LICENSE). Third-party components keep their own licenses (Apache Kafka, Apache Spark and
Apache Iceberg under Apache-2.0, RustFS under Apache-2.0, kafbat/kafka-ui under Apache-2.0); see
[NOTICE](NOTICE). Apache®, Apache Kafka®, Apache Spark® and Apache Iceberg® are trademarks of the Apache
Software Foundation; Cloudera and CDP are trademarks of Cloudera, Inc. No endorsement is implied. All
telemetry in this repository is synthetic.
