#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_DIR/package

# remove contents under $SRC_PATH (except for pyflink-pipeline)
shopt -s extglob
rm -rf $SRC_PATH/!(pyflink-pipeline)

## Generate Uber Jar for PyFlink app for MSK cluster with IAM authN
echo "generate Uber jar for PyFlink app..."
mkdir $SRC_PATH/lib
mvn clean install -f $SRC_PATH/pyflink-pipeline/pom.xml \
  && mv $SRC_PATH/pyflink-pipeline/target/pyflink-pipeline-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/pyflink-pipeline/target
