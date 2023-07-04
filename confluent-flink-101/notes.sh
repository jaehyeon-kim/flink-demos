# build docker image
docker build -t=confluent-flink-101:1.15.4 .

# start docker compose services and run sql client
docker-compose -f compose-kafka.yml up -d
docker-compose -f compose-flink-linked.yml up -d
docker-compose -f compose-flink-linked.yml run sql-client


docker run --rm -it --network=kafka-network bitnami/kafka:2.8.1 \
  /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-0:9092 \
  --create --topic pageviews
