#!/usr/bin/env bash

cd /tachyon
mvn clean install -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true >mvn.log 2>&1
