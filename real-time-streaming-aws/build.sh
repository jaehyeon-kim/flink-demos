#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_DIR/package

# remove contents under $SRC_PATH (except for kafka-connector-with-iam-auth) and kda-package.zip file
shopt -s extglob
rm -rf $SRC_PATH/!(kafka-connector-with-iam-auth) kda-package.zip

## Generate Uber Jar for PyFlink app for MSK cluster with IAM authN
echo "generate Uber jar for PyFlink app..."
mkdir $SRC_PATH/lib
mvn clean install -f $SRC_PATH/kafka-connector-with-iam-auth/pom.xml \
  && mv $SRC_PATH/kafka-connector-with-iam-auth/target/kafka-connector-with-iam-auth-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/kafka-connector-with-iam-auth/target

## Package s3 loader pyflink app
echo "package pyflink app"
zip -r kda-package.zip loader/processor.py package/lib
