#!/bin/sh
# Create the lab topics. Idempotent. Partition count = parallelism of the Kafka source.
set -e
K=/opt/kafka/bin/kafka-topics.sh
B=kafka:9092
for i in $(seq 1 60); do $K --bootstrap-server $B --list >/dev/null 2>&1 && break; sleep 1; done
$K --bootstrap-server $B --create --if-not-exists --topic cell.kpi    --partitions 6 --replication-factor 1 --config retention.ms=86400000
$K --bootstrap-server $B --create --if-not-exists --topic cell.alerts --partitions 3 --replication-factor 1 --config cleanup.policy=compact,delete
# Avro copy of the KPI stream (Confluent wire format, schema ids from the registry).
$K --bootstrap-server $B --create --if-not-exists --topic cell.kpi.avro --partitions 6 --replication-factor 1 --config retention.ms=86400000
# Engine comparison sinks: the same 1-minute per-cell aggregation from Spark and from Flink.
for t in kpi_1m.spark kpi_1m.flink kpi_1m.flink_eo eo.flink.exactly_once eo.flink.at_least_once eo.spark.kafka; do
  $K --bootstrap-server $B --create --if-not-exists --topic $t --partitions 3 --replication-factor 1 --config retention.ms=86400000
done
$K --bootstrap-server $B --list
