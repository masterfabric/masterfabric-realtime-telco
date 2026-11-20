#!/usr/bin/env bash
# One end-to-end live run: fresh topic, streaming app, then the producer on the simulated clock.
# usage: scripts/live_run.sh <run_id> <namespace> <topic> [producer args...]
set -euo pipefail
RUN="$1"; NS="$2"; TOPIC="$3"; shift 3
K="docker exec mf-rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092"
$K --delete --if-exists --topic "$TOPIC" >/dev/null 2>&1 || true
for i in $(seq 1 30); do $K --list | grep -qx "$TOPIC" || break; sleep 1; done
$K --create --topic "$TOPIC" --partitions 6 --replication-factor 1 --config retention.ms=86400000 >/dev/null
rm -rf "data/runs/$RUN"; mkdir -p "data/runs/$RUN"
docker exec -d -w /lab mf-rt-spark bash -c "spark-submit --master "local[4]" --driver-memory \${SPARK_DRIVER_MEMORY:-3g} \
  streaming/app.py --run-id $RUN --ns $NS --topic $TOPIC --reset ${APP_ARGS:-} > /data/runs/$RUN/app.log 2>&1"
# Wait until all three queries are running before producing, so startup does not count as latency.
for i in $(seq 1 120); do grep -q "QUERIES_STARTED" "data/runs/$RUN/app.log" 2>/dev/null && break; sleep 1; done
docker exec -w /lab mf-rt-spark python -m producer.run --topic "$TOPIC" --run-id "$RUN" "$@" 2>&1 | grep -v GETPID | tail -1
while docker exec mf-rt-spark pgrep -f "run-id $RUN" >/dev/null; do sleep 3; done
tail -1 "data/runs/$RUN/app.log"
