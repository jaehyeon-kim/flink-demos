#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_DIR/package

# remove contents under $SRC_PATH (except for the folders beginging with lab)
shopt -s extglob
rm -rf $SRC_PATH/!(lab*)

## Generate Uber Jar for PyFlink app for MSK cluster with IAM authN
echo "generate Uber jar for PyFlink app..."
mkdir $SRC_PATH/lib
mvn clean install -f $SRC_PATH/lab2-pipeline/pom.xml \
  && mv $SRC_PATH/lab2-pipeline/target/lab2-pipeline-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/lab2-pipeline/target

mvn clean install -f $SRC_PATH/lab3-pipeline/pom.xml \
  && mv $SRC_PATH/lab3-pipeline/target/lab3-pipeline-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/lab3-pipeline/target