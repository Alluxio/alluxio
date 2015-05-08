#!/usr/bin/env bash

# build tachyon pkg
cd /tachyon
echo "compiling Tachyon..."
mvn -q clean install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0 -DskipTests -Dmaven.javadoc.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true
