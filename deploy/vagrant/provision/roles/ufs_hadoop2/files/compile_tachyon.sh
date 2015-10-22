#!/usr/bin/env bash

cd /tachyon
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Dlicense.skip=true -Dcheckstyle.skip=true -Pmesos -Pyarn >mvn.log 2>&1
