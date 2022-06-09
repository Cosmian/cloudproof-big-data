#!/bin/bash

set -e

if [ -z "$SPARK_HOME" ]; then
    echo "Please set the variable SPARK_HOME to the location of the Spark directory"
    exit 1
else
    echo "Using Spark at ${SPARK_HOME}"
fi

$SPARK_HOME/bin/spark-submit \
    --class com.cosmian.cloudproof_demo.Spark \
    --master local[*] \
    target/cloudproof-demo-2.0.0.jar $@