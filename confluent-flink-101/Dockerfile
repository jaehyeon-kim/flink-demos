FROM flink:1.15.4-scala_2.12

RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.15.4/flink-connector-kafka-1.15.4.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar; \
  wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.4/flink-sql-connector-kafka-1.15.4.jar; \
  wget -P /opt/flink/lib/ https://github.com/knaufk/flink-faker/releases/download/v0.5.0/flink-faker-0.5.0.jar;
