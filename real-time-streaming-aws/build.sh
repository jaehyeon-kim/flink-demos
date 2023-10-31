#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"
SRC_PATH=$SCRIPT_DIR/package

# remove contents under $SRC_PATH (except for s3-data-loader) and kda-package.zip file
shopt -s extglob
rm -rf $SRC_PATH/!(s3-data-loader) loader-package.zip

## Generate Uber Jar for PyFlink app for MSK cluster with IAM authN
echo "generate Uber jar for PyFlink app..."
mkdir $SRC_PATH/lib
mvn clean install -f $SRC_PATH/s3-data-loader/pom.xml \
  && mv $SRC_PATH/s3-data-loader/target/s3-data-loader-1.0.0.jar $SRC_PATH/lib \
  && rm -rf $SRC_PATH/s3-data-loader/target

# ## Package s3 loader pyflink app
# echo "package s3 loader pyflink app"
# zip -r loader-package.zip loader/processor.py package/lib

## Package s3 loader pyflink app
echo "package s3 loader pyflink app"
cd loader \
  && zip loader-package.zip processor.py \
  && mv loader-package.zip .. \
  && cd .. \
  && zip loader-package.zip package/lib/s3-data-loader-1.0.0.jar