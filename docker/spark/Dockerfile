# Spark 3.5.5 + Iceberg 1.8.1 runtime (Apache-2.0 image by Tabular) plus the Kafka source/sink
# for Structured Streaming, spark-avro for the Avro topic, and the Python Kafka and Avro clients
# used by the producer and the test harnesses.
FROM tabulario/spark-iceberg:3.5.5_1.8.1
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO $MVN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar \
 && curl -fsSLO $MVN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO $MVN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
 && curl -fsSLO $MVN/org/apache/spark/spark-avro_2.12/3.5.5/spark-avro_2.12-3.5.5.jar
RUN pip install --no-cache-dir pytest==8.3.5 confluent-kafka==2.6.1 fastavro==1.10.0
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties
