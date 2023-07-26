#!/usr/bin/env bash
PKG_ALL="${PKG_ALL:-no}"

SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_DIR/package

# remove contents under $SRC_PATH except for uber-jar-for-pyflink and kda-package.zip
shopt -s extglob
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

## Package pyflink app
echo "package pyflink app"
zip -r kda-package.zip processor.py package/lib package/site_packages
