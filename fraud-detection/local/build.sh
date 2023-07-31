#!/usr/bin/env bash
PKG_ALL="${PKG_ALL:-no}"

SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

#### Steps to package the flink app
SRC_PATH=$SCRIPT_DIR/package
rm -rf $SRC_PATH && mkdir -p $SRC_PATH/lib

## Download flink sql connector kafka
echo "download flink sql connector kafka..."
VERSION=1.15.2
FILE_NAME=flink-sql-connector-kafka-$VERSION
FLINK_SRC_DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/$VERSION/flink-sql-connector-kafka-$VERSION.jar
curl -L -o $SRC_PATH/lib/$FILE_NAME.jar ${FLINK_SRC_DOWNLOAD_URL}

## Install pip packages
echo "install and zip pip packages..."
pip3 install -r requirements.txt --target $SRC_PATH/site_packages

if [ $PKG_ALL == "yes" ]; then
  ## Package pyflink app
  echo "package pyflink app"
  zip -r kda-package.zip processor.py package/lib package/site_packages
fi

#### Steps to create the sink connector
CONN_PATH=$SCRIPT_DIR/connectors
rm -rf $CONN_PATH && mkdir $CONN_PATH

## Download camel dynamodb sink connector
echo "download camel dynamodb sink connector..."
CONNECTOR_SRC_DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/camel/kafkaconnector/camel-aws-ddb-sink-kafka-connector/3.20.3/camel-aws-ddb-sink-kafka-connector-3.20.3-package.tar.gz

## decompress and zip contents to create custom plugin of msk connect later
curl -o $CONN_PATH/camel-aws-ddb-sink-kafka-connector.tar.gz $CONNECTOR_SRC_DOWNLOAD_URL \
  && tar -xvzf $CONN_PATH/camel-aws-ddb-sink-kafka-connector.tar.gz -C $CONN_PATH \
  && cd $CONN_PATH/camel-aws-ddb-sink-kafka-connector \
  && zip -r camel-aws-ddb-sink-kafka-connector.zip . \
  && mv camel-aws-ddb-sink-kafka-connector.zip $CONN_PATH \
  && rm $CONN_PATH/camel-aws-ddb-sink-kafka-connector.tar.gz