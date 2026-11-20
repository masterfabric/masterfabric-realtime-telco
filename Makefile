# MasterFabric Realtime Telco. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml
SPARK   := docker exec -w /lab mf-rt-spark
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs topics produce test-unit offline live evaluate maintain

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

env: ## Create .env from .env.example (never overwrites)
	@test -f .env || cp .env.example .env

up: env ## Start Kafka, Kafka UI, object storage, the Iceberg catalog and Spark
	mkdir -p data
	$(COMPOSE) up -d --build
	@echo "Kafka UI http://127.0.0.1:28080  S3 console http://127.0.0.1:29001  Spark UI http://127.0.0.1:24040  history http://127.0.0.1:28090"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Tail service logs
	$(COMPOSE) logs -f --tail=50

topics: ## Describe the Kafka topics
	docker exec mf-rt-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

CELLS ?= 240
HOURS ?= 6
SPEED ?= 120
RUN   ?= live

produce: ## Stream synthetic KPI reports into Kafka (CELLS=240 HOURS=6 SPEED=120 RUN=live)
	$(SPARK) python -m producer.run --cells $(CELLS) --hours $(HOURS) --speed $(SPEED) --run-id $(RUN)

test-unit: ## Producer and detector unit tests (inside the Spark container)
	$(SPARK) python -m pytest -q -p no:cacheprovider tests/unit

SEED ?= 7

offline: ## Replay a producer file dump through the detector with pandas (SEED=7); tuning only
	$(SPARK) bash -c 'python -m producer.run --sink file --speed 0 --seed $(SEED) --run-id dev-seed$(SEED) >/dev/null && \
	  python -m streaming.offline --events /data/runs/dev-seed$(SEED)/events.jsonl --sweep-cusum 8,10,12,14,16 --out docs/results/offline_dev_seed$(SEED).json'

LIVE_ARGS ?= --cells 240 --hours 6 --speed 30 --seed 2026

live: ## End-to-end live run on a fresh topic (RUN=live), held-out seed 2026, 6 simulated hours at 30x
	APP_ARGS="--trigger-bronze 5 --trigger-silver 2 --trigger-detect 2" scripts/live_run.sh $(RUN) rt cell.kpi $(LIVE_ARGS)

evaluate: ## Reconcile, score and time a finished live run into docs/results/live_$(RUN).json
	$(SPARK) spark-submit --master 'local[4]' streaming/evaluate.py --run-id $(RUN) --ns rt --out docs/results/live_$(RUN).json

maintain: ## Compact streaming small files and expire snapshots, with before/after numbers
	$(SPARK) spark-submit --master 'local[4]' streaming/maintenance.py --ns rt --out docs/results/maintenance_$(RUN).json
