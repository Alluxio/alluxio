#!/bin/sh
set -e

HADOOP_VERSION="2.4.1"

# setup tachyon
cd /tachyon
echo "compiling Tachyon..."
mvn -q clean install -Dmaven.javadoc.skip=true -DskipTests -Dhadoop.version=${HADOOP_VERSION} -Dlicense.skip=true -Dcheckstyle.skip=true
