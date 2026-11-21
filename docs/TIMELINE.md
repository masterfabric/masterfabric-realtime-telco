# Build timeline

The lab was built in small steps on 2026-11-21: infrastructure first, then the producer and its tests, the detector (tuned offline), the streaming application, ADRs and maintenance. The first full live run exposed a CUSUM recovery bug, which was fixed and then measured again on the held-out seed. Benchmarks, results, visuals and documentation came last.

Every step below is one commit, in order. Times are Europe/Istanbul (UTC+3).

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `447c4ac` | 2026-11-21 02:15 | chore: scaffold repository with AGPL-3.0 license and community files |
| 2 | `929ae19` | 2026-11-21 02:18 | feat(infra): add Kafka, Iceberg REST catalog, S3 storage and Spark streaming stack |
| 3 | `ffd85d6` | 2026-11-21 02:21 | feat(producer): simulate a radio network and stream KPI reports into Kafka |
| 4 | `5feac08` | 2026-11-21 02:24 | feat(detection): add per-cell adaptive anomaly detector with Bernoulli CUSUM |
| 5 | `de652ff` | 2026-11-21 02:37 | feat(streaming): chain Kafka, Iceberg bronze/silver and stateful detection |
| 6 | `0327c37` | 2026-11-21 02:46 | docs(adr): record seven architecture decisions for the streaming lab |
| 7 | `8242c57` | 2026-11-21 02:46 | feat(ops): add Iceberg maintenance job for streaming small files |
| 8 | `05e1f77` | 2026-11-21 02:53 | fix(detection): cap the CUSUM so degraded cells recover after repair |
| 9 | `525233b` | 2026-11-21 02:54 | fix(infra): expose the streaming application's Spark UI on 4041 |
| 10 | `7a20587` | 2026-11-21 03:12 | perf(bench): measure drain throughput for bronze, silver and both |
| 11 | `3c7bb44` | 2026-11-21 03:12 | docs(results): record the held-out live run, latency and maintenance |
| 12 | `545a35e` | 2026-11-21 03:12 | docs(visuals): add diagrams, result charts, UI screenshots and hero |
| 13 | `dbed340` | 2026-11-21 03:14 | docs: add contributing guide and itemise third-party licenses in NOTICE |

The final commit adds this file and the README; see `git log` for the full messages, which explain
the why of each step (including the fixes found by running the system).

## Addition: schema registry, Flink vs Spark, exactly-once tests

Added on 2026-11-21 in 19 commits (this file's update is the last). Apicurio Registry and Flink 2.2 joined
the Compose project behind two profiles; the producer gained an Avro mode; the same silver aggregation
was written for Spark and for Flink SQL; a harness measured throughput, latency, window equality and
duplicates after `kill -9`. Problems found on the way are in the commit messages and the README:
an Avro enum that Flink cannot resolve to a string (changed to string), a Spark union that read Kafka
twice per batch (replaced by one `CASE`), idle progress events that kept the Spark job from stopping,
a latency measure skewed by dropped very late records (now on-time records only), a recovery timer that
read a stale RUNNING state (now waits for the recorded failure), and a registry CORS setting that only
works through `QUARKUS_HTTP_CORS_ORIGINS`.

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `15b153e` | 2026-11-21 09:46 | Add Apicurio schema registry and a Flink 2.2 session cluster as compose profiles |
| 2 | `0d742c6` | 2026-11-21 09:48 | Add Avro schemas v1, v2 (compatible) and v3 (incompatible); spark-avro and fastavro in the Spark image |
| 3 | `f4e0c60` | 2026-11-21 09:48 | Producer: --format avro writes the Confluent wire format with registry-assigned ids, optional v2 switch mid-run |
| 4 | `4c55274` | 2026-11-21 09:48 | Registry demo: BACKWARD compatibility accepts v2 and rejects v3 with 409 |
| 5 | `b677b75` | 2026-11-21 09:52 | Silver aggregation over the Avro topic in Spark (from_avro per schema id) and in Flink SQL (avro-confluent) |
| 6 | `4204b37` | 2026-11-21 09:57 | Engine comparison harness; drain benchmark: Flink 3.07 s vs Spark 10.54 s on 1.04M Avro records, identical windows |
| 7 | `f5c3b55` | 2026-11-21 10:13 | Live comparison at 1,449 events/s: p50 latency Flink 2.6 s, Spark 4.1 s, Flink exactly-once 7.7 s; identical windows |
| 8 | `1c4f1a1` | 2026-11-21 10:24 | Flink kill test: 2 TaskManager kills, exactly-once sink 0 duplicates (read_committed), at-least-once 4,080 |
| 9 | `487ac7b` | 2026-11-21 10:26 | Spark kill test: 3 driver kills, Iceberg bronze holds all 521,044 offsets exactly once |
| 10 | `35b36dd` | 2026-11-21 10:29 | Kafka UI decodes the Avro topic through the registry; registry CORS via QUARKUS_HTTP_CORS_ORIGINS for its UI |
| 11 | `52b181f` | 2026-11-21 10:33 | Screenshots: Flink job graph, exceptions and checkpoints after a TaskManager kill; Apicurio versions, rules, v2; Kafka UI decoding Avro |
| 12 | `d9f13bf` | 2026-11-21 10:34 | Unit tests for the Avro wire format and evolution rules; producer keeps JSON as the default for older callers |
| 13 | `6cd2e3a` | 2026-11-21 10:35 | Make targets for the registry, engine comparison and kill tests; diagram and charts |
| 14 | `c7296cd` | 2026-11-21 10:35 | Create kpi_1m.flink_eo with the other lab topics |
| 15 | `a7f0303` | 2026-11-21 10:37 | ADR 0008 Avro and schema registry |
| 16 | `9871326` | 2026-11-21 10:37 | ADR 0009 Flink vs Spark measured, ADR 0010 kill tests, ADR index |
| 17 | `b793354` | 2026-11-21 10:37 | NOTICE: Flink, Flink Kafka/Avro-registry jars, Apicurio, spark-avro, fastavro |
| 18 | `06a0aec` | 2026-11-21 10:39 | README: schema registry, Flink vs Spark and kill-test results with figures; quickstart, ADRs, limitations, roadmap; fix a Mermaid parse error |

Verification at the end of the addition:

| Check | Result |
|---|---|
| Unit tests in the Spark container | 25 passed |
| Registry demo | v2 accepted, v3 rejected with 409 |
| Spark vs Flink windows | 172,080 / 172,080 (drain), 28,080 / 28,080 (live) identical |
| Kill tests | Flink exactly-once 0 duplicates, at-least-once 4,080; Spark → Iceberg 521,044 / 521,044 offsets |
| Mermaid blocks in the README | 3 render with mermaid-cli |
| Machine state after the run | realtime containers stopped, Colima running, k3d cluster stopped |
