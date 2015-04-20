#!/usr/bin/env bash

HADOOP_VERSION="1.0.4"
# setup tachyon
cd /tachyon
echo "compiling Tachyon..."
mvn -q clean package install -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true
