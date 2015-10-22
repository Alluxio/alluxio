#!/usr/bin/env bash

cd /tachyon
mvn -q clean install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0 -DskipTests -Dmaven.javadoc.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Pmesos >mvn.log  2>&1
