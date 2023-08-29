#!/usr/bin/env bash
shopt -s extglob

PKG_ALL="${PKG_ALL:-yes}"
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

#### Steps to package the flink app
# remove contents under $SRC_PATH (except for uber-jar-for-pyflink) and kda-package.zip file
SRC_PATH=$SCRIPT_DIR/package
rm -rf $SRC_PATH/!(uber-jar-for-pyflink) kda-package.zip

## Generate Uber Jar for PyFlink app for MSK cluster with IAM authN
echo "generate Uber jar for PyFlink app..."
mkdir $SRC_PATH/lib
mvn clean install -f $SRC_PATH/uber-jar-for-pyflink/pom.xml \
  && mv $SRC_PATH/uber-jar-for-pyflink/target/pyflink-getting-started-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/uber-jar-for-pyflink/target

## Install pip packages
echo "install and zip pip packages..."
pip install -r requirements.txt --target $SRC_PATH/site_packages

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