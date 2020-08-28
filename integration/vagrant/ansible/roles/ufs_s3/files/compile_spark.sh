#!/usr/bin/env bash

cd /spark
build/mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Phive -Phive-thriftserver > compile.log 2>&1
