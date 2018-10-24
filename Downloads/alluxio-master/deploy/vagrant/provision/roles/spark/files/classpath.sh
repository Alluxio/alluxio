#!/usr/bin/env bash

echo export SPARK_CLASSPATH=/alluxio/core/client/target/alluxio-core-client-*-jar-with-dependencies.jar:${SPARK_CLASSPATH} >> /spark/conf/spark-env.sh
