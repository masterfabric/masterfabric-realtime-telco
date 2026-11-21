# Apache Flink 2.2 (Apache-2.0) with the Kafka SQL connector and the Avro format that resolves
# writer schemas from a Confluent-compatible schema registry (Apicurio's ccompat API here).
FROM flink:2.2.1-java17
ARG MVN=https://repo1.maven.org/maven2/org/apache/flink
RUN cd /opt/flink/lib \
 && wget -q $MVN/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
 && wget -q $MVN/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar \
 && chown flink:flink /opt/flink/lib/*.jar
