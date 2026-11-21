"""Exactly-once check for a bronze table: every Kafka offset exactly once, none missing.

    spark-submit streaming/eo_check.py --table lake.eo.kpi_events
"""
import argparse
import json

from pyspark.sql import SparkSession


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.appName("eo-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    r = spark.sql(f"""
        SELECT count(*) AS rows,
               count(DISTINCT kafka_partition, kafka_offset) AS distinct_offsets,
               sum(CASE WHEN parse_ok THEN 0 ELSE 1 END) AS unparsed
        FROM {a.table}""").first().asDict()
    gaps = spark.sql(f"""
        SELECT kafka_partition, min(kafka_offset) AS lo, max(kafka_offset) AS hi, count(*) AS n
        FROM {a.table} GROUP BY kafka_partition""").collect()
    r["offset_gaps"] = sum(int(g.hi - g.lo + 1 - g.n) for g in gaps)
    r["snapshots"] = spark.sql(f"SELECT count(*) AS n FROM {a.table}.snapshots").first().n
    print(json.dumps(r))


if __name__ == "__main__":
    main()
