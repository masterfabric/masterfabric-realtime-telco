#!/usr/bin/env bash
# Throughput benchmark: pre-fill a topic at full producer speed, then drain it with availableNow
# triggers in several configurations. Results: docs/results/bench_throughput.json
set -euo pipefail
HOURS="${HOURS:-12}"; CELLS="${CELLS:-240}"; TOPIC=cell.kpi.bench; RUN=bench
K="docker exec mf-rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092"
$K --delete --if-exists --topic $TOPIC >/dev/null 2>&1 || true
for i in $(seq 1 30); do $K --list | grep -qx $TOPIC || break; sleep 1; done
$K --create --topic $TOPIC --partitions 6 --replication-factor 1 >/dev/null
rm -rf data/runs/$RUN; mkdir -p data/runs/$RUN
docker exec -w /lab mf-rt-spark python -m producer.run --topic $TOPIC --run-id $RUN --cells $CELLS --hours $HOURS \
  --speed 0 --seed 99 2>&1 | grep -v GETPID | tail -1
run() {  # label queries max_offsets
  docker exec -w /lab mf-rt-spark spark-submit --master "local[4]" --driver-memory 3g streaming/app.py \
    --run-id $RUN --ns "bench_$1" --topic $TOPIC --queries "$2" --max-offsets "$3" --available-now --reset \
    --idle-stop-s 0 > "data/runs/$RUN/app_$1.log" 2>&1
  echo "done $1"
}
run bronze_100k bronze 100000
run silver_100k silver 100000
run silver_500k silver 500000
run both_100k bronze,silver 100000
docker exec -w /lab mf-rt-spark python scripts/bench_summary.py
