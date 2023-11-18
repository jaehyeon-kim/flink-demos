#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

SRC_PATH=${SCRIPT_DIR}/infra/connectors
rm -rf ${SRC_PATH} && mkdir -p ${SRC_PATH}

## Download camel dynamodb sink connector
echo "download camel dynamodb sink connector..."
DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-aws-ddb-sink-kafka-connector/3.20.3/camel-aws-ddb-sink-kafka-connector-3.20.3-package.tar.gz

# decompress and zip contents to create custom plugin of msk connect later
curl -o ${SRC_PATH}/camel-aws-ddb-sink-kafka-connector.tar.gz ${DOWNLOAD_URL} \
  && tar -xvzf ${SRC_PATH}/camel-aws-ddb-sink-kafka-connector.tar.gz -C ${SRC_PATH} \
  && cd ${SRC_PATH}/camel-aws-ddb-sink-kafka-connector \
  && zip -r camel-aws-ddb-sink-kafka-connector.zip . \
  && mv camel-aws-ddb-sink-kafka-connector.zip ${SRC_PATH} \
  && rm ${SRC_PATH}/camel-aws-ddb-sink-kafka-connector.tar.gz