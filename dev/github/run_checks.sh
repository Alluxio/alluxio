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

if [ -n "${ALLUXIO_GIT_CLEAN}" ]
then
  # https://stackoverflow.com/questions/72978485/git-submodule-update-failed-with-fatal-detected-dubious-ownership-in-repositor
  git config --global --add safe.directory '*'
  git clean -fdx
fi

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

# Always use java 8 to compile the source code
JAVA_HOME_BACKUP=${JAVA_HOME}
PATH_BACKUP=${PATH}
JAVA_HOME=/usr/local/openjdk-8
PATH=$JAVA_HOME/bin:$PATH
mvn -Duser.home=/home/jenkins -T 4C clean install -DskipTests ${mvn_args}

./dev/scripts/check-docs.sh
./dev/scripts/build-artifact.sh ufsVersionCheck

