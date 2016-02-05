#!/usr/bin/env bash

echo export SPARK_CLASSPATH=/alluxio/client/target/alluxio-client-*-jar-with-dependencies.jar:$SPARK_CLASSPATH >> /spark/conf/spark-env.sh
