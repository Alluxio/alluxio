#!/usr/bin/env bash

cd /spark
build/mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Phadoop-${SPARK_PROFILE} -Pyarn -Phive -Phive-thriftserver > compile.log 2 > &1
