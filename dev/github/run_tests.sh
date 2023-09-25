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

if [ -z "${ALLUXIO_FORK_COUNT}" ]; then
  ALLUXIO_FORK_COUNT=2
fi

if [ -n "${ALLUXIO_GIT_CLEAN}" ]; then
  # https://stackoverflow.com/questions/72978485/git-submodule-update-failed-with-fatal-detected-dubious-ownership-in-repositor
  git config --global --add safe.directory '*'
  git clean -fdx
fi

mvn_args=""
if [ -n "${ALLUXIO_MVN_PROJECT_LIST}" ]; then
  mvn_args+="-am -pl ${ALLUXIO_MVN_PROJECT_LIST}"
fi

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

# Always use java 8 to compile the source code
JAVA_HOME_BACKUP=${JAVA_HOME}
PATH_BACKUP=${PATH}
JAVA_HOME=/usr/local/openjdk-8
PATH=$JAVA_HOME/bin:$PATH
mvn -Duser.home=/home/jenkins -T 4C clean install -Dfindbugs.skip -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip \
-Dlicense.skip -Dsort.skip ${mvn_args}

# Set things up so that the current user has a real name and can authenticate.
myuid=$(id -u)
mygid=$(id -g)
echo "$myuid:x:$myuid:$mygid:anonymous uid:/home/jenkins:/bin/false" >> /etc/passwd

# Revert back to the image default java version to run the test
JAVA_HOME=${JAVA_HOME_BACKUP}
PATH=${PATH_BACKUP}

mvn_args+=" -fn -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false --fail-at-end"
if [ -n "${ALLUXIO_MVN_TESTS}" ]; then
  mvn_args+=" -Dtest=${ALLUXIO_MVN_TESTS}"
fi

# Run tests
mvn -Duser.home=/home/jenkins test -Dmaven.main.skip -Dskip.protoc=true -Dmaven.javadoc.skip -Dlicense.skip=true \
-Dcheckstyle.skip=true -Dfindbugs.skip=true -Dsort.skip -Dsurefire.forkCount=${ALLUXIO_FORK_COUNT} ${mvn_args}
