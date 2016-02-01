#!/usr/bin/env bash

cd /tachyon
mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos >mvn.log 2>&1
