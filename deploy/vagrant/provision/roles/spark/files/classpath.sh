#!/usr/bin/env bash

echo export SPARK_CLASSPATH=/tachyon/client/target/tachyon-client-*-jar-with-dependencies.jar:$SPARK_CLASSPATH >> /spark/conf/spark-env.sh
