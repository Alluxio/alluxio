#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

#
# This script is run from inside the Docker container
#
set -ex

./dev/github/helper.sh

mvn_args=""
if [ -n "${ALLUXIO_MVN_RUNTOEND}" ]
then
  mvn_args+=" -fn -DfailIfNoTests=false --fail-at-end"
fi

# Set things up so that the current user has a real name and can authenticate.
myuid=$(id -u)
mygid=$(id -g)
echo "$myuid:x:$myuid:$mygid:anonymous uid:/home/jenkins:/bin/false" >> /etc/passwd

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

mvn -Duser.home=/home/jenkins test -Pdeveloper -Dmaven.main.skip -Dskip.protoc=true  -Dmaven.javadoc.skip -Dlicense.skip=true \
-Dcheckstyle.skip=true -Dfindbugs.skip=true -Dsurefire.forkCount=2 ${mvn_args} $@
