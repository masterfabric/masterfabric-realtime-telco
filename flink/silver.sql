-- Flink side of the engine comparison: the same silver aggregation as streaming/avro_silver.py.
-- Rendered by scripts/flink_job.py ({{...}} placeholders) and submitted with the SQL client.
--
--   Kafka cell.kpi.avro (avro-confluent: writer schema fetched by id, reader schema = this DDL)
--     ──► watermark 2 min ──► window dedupe on event_id ──► 1-minute tumbling windows per cell
--     ──► Kafka {{OUT_TOPIC}} (JSON), delivery guarantee {{GUARANTEE}}

SET 'pipeline.name' = '{{JOB_NAME}}';
SET 'parallelism.default' = '6';
SET 'execution.checkpointing.interval' = '{{CHECKPOINT}}';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout' = '10 s';
SET 'execution.runtime-mode' = 'streaming';

CREATE TABLE kpi (
  v                  INT,
  event_id           STRING,
  event_ts           TIMESTAMP(3),
  cell_id            STRING,
  site_id            STRING,
  city               STRING,
  rat                STRING,
  interval_s         INT,
  active_users       DOUBLE,
  prb_util_pct       DOUBLE,
  dl_throughput_mbps DOUBLE,
  ul_sinr_db         DOUBLE,
  latency_ms         DOUBLE,
  rrc_attempts       BIGINT,
  rrc_success        BIGINT,
  erab_drops         BIGINT,
  availability_pct   DOUBLE,
  energy_kwh         DOUBLE,          -- v2 only; null for v1 records (schema resolution)
  WATERMARK FOR event_ts AS event_ts - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = '{{IN_TOPIC}}',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '{{JOB_NAME}}',
  'scan.startup.mode' = 'earliest-offset',
  {{BOUNDED}}
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://registry:8080/apis/ccompat/v7'
);

CREATE TABLE kpi_1m (
  window_start        STRING,
  cell_id             STRING,
  reports             BIGINT,
  users_avg           DOUBLE,
  prb_avg             DOUBLE,
  prb_max             DOUBLE,
  throughput_avg_mbps DOUBLE,
  latency_avg         DOUBLE,
  rrc_attempts        BIGINT,
  rrc_success         BIGINT,
  erab_drops          BIGINT,
  availability_min    DOUBLE,
  energy_kwh_sum      DOUBLE,
  v2_reports          BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '{{OUT_TOPIC}}',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.transaction.timeout.ms' = '600000',
  'sink.delivery-guarantee' = '{{GUARANTEE}}',
  'sink.transactional-id-prefix' = '{{JOB_NAME}}',
  'key.format' = 'raw',
  'key.fields' = 'window_start',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

INSERT INTO kpi_1m
SELECT DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss'), cell_id,
       COUNT(*), AVG(active_users), AVG(prb_util_pct), MAX(prb_util_pct), AVG(dl_throughput_mbps),
       AVG(latency_ms), SUM(rrc_attempts), SUM(rrc_success), SUM(erab_drops), MIN(availability_pct),
       SUM(energy_kwh), SUM(CASE WHEN v = 2 THEN 1 ELSE 0 END)
FROM (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end, event_id ORDER BY event_ts ASC) AS rn
    FROM TABLE(TUMBLE(TABLE kpi, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
  ) WHERE rn = 1
)
GROUP BY window_start, window_end, cell_id;
