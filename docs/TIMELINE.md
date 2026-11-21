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
