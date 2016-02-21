#!/usr/bin/env bash

cd /alluxio
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos >mvn.log 2>&1
