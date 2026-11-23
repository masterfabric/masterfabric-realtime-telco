#!/usr/bin/env bash
# Backpressure: the silver query alone, read cap fixed at MAX_OFFSETS per 2-second trigger
# (1,000 records/s), fed at 0.5x, 1x, 1.5x and 2x that rate, plus 2x without the cap.
# Each producer run is one simulated hour of 240 cells; results -> docs/results/backpressure.json
set -euo pipefail
cd "$(dirname "$0")/.."
TOPIC=cell.kpi.bp
RUNS=()
for cfg in "bp-s20:20:2000" "bp-s40:40:2000" "bp-s60:60:2000" "bp-s80:80:2000" "bp-s80-uncapped:80:100000"; do
  IFS=: read -r RUN SPEED MAX <<<"$cfg"
  RUNS+=("data/runs/$RUN/backpressure.json")
  if [[ -n "${ONLY_MISSING:-}" && -f "data/runs/$RUN/backpressure.json" ]]; then continue; fi
  APP_ARGS="--queries silver --trigger-silver 2 --max-offsets $MAX --idle-stop-s 15" \
    scripts/live_run.sh "$RUN" bp "$TOPIC" --cells 240 --hours 1 --warmup-min 5 --speed "$SPEED" --seed 2026
  docker exec -w /lab mf-rt-spark spark-submit --master 'local[2]' streaming/backpressure_report.py --run-id "$RUN" --ns bp 2>&1 | grep '^{'
done
python3 - "${RUNS[@]}" <<'PY'
import json, sys
json.dump({"runs": [json.load(open(p)) for p in sys.argv[1:]]}, open("docs/results/backpressure.json", "w"), indent=1)
PY
docker exec mf-rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic "$TOPIC" || true
