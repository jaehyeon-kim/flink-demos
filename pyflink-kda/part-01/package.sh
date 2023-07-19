#!/usr/bin/env bash
PKG_ALL="${PKG_ALL:-no}"

SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

SRC_PATH=$SCRIPT_DIR
rm -rf $SRC_PATH/lib $SRC_PATH/site_packages $SRC_PATH/kda-package.zip \
  && mkdir -p $SRC_PATH/lib

## Download flink sql connector kafka
echo "download flink sql connector kafka..."
VERSION=1.15.2
FILE_NAME=flink-sql-connector-kafka-$VERSION
DOWNLOAD_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/$VERSION/flink-sql-connector-kafka-$VERSION.jar
curl -L -o $SRC_PATH/lib/$FILE_NAME.jar ${DOWNLOAD_URL}

if [ $PKG_ALL == "yes" ]; then
  ## Install pip packages
  echo "install and zip pip packages..."
  pip install -r requirements.txt --target $SRC_PATH/site_packages

  ## Package pyflink app
  echo "package pyflink app"
  zip -r kda-package.zip lib site_packages processor.py application_properties.json
fi

