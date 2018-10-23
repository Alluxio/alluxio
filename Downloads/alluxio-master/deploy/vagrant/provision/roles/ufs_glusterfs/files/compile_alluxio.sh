#!/usr/bin/env bash

cd /alluxio
mvn -q clean install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0 -DskipTests -Dmaven.javadoc.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos > mvn.log 2>&1
