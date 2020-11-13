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
  git clean -ffdx
fi

mvn_args=""
if [ -n "${ALLUXIO_MVN_RUNTOEND}" ]
then
  mvn_args+=" -Dmaven.test.failure.ignore=true -fn --fail-at-end"
fi

RUN_MAVEN="false"
RUN_DOC_CHECK="false"
if [ -z "${TARGET_BRANCH}" ]; then
  # if no target branch is specified, run all checks
  RUN_MAVEN="true"
  RUN_DOC_CHECK="true"
else
  git --no-pager diff "refs/remotes/origin/${TARGET_BRANCH}" --name-only > prFiles.diff
  echo "PR diff is:"
  cat prFiles.diff

  while IFS="" read -r filepath || [ -n "$filepath" ]; do
    if [[ ${filepath} =~ ^dev/* ]]; then
      # run all checks if modifying any part of the build process
      RUN_MAVEN="true"
      RUN_DOC_CHECK="true"
    elif [[ ${filepath} =~ ^docs/.* ]]; then
      # if any file starts with "docs/", run doc check
      RUN_DOC_CHECK="true"
    elif [[ ${filepath} =~ ^integration/(dataproc|docker|emr|kubernetes|vagrant)/.* ]]; then
      # do nothing for files in integration/ that don't contain java code
      :
    else
      # if any other files are in the diff, run maven
      RUN_MAVEN="true"
    fi
  done < prFiles.diff
  rm prFiles.diff
fi

# Set things up so that the current user has a real name and can authenticate.
myuid=$(id -u)
mygid=$(id -g)
echo "$myuid:x:$myuid:$mygid:anonymous uid:/home/jenkins:/bin/false" >> /etc/passwd

export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

if [ -z "${ALLUXIO_BUILD_FORKCOUNT}" ]
then
  ALLUXIO_BUILD_FORKCOUNT=1
fi
if [ "$RUN_MAVEN" == "true" ]; then
  # Always use java 8 to compile the source code
  JAVA_HOME_BACKUP=${JAVA_HOME}
  PATH_BACKUP=${PATH}
  JAVA_HOME=/usr/local/openjdk-8
  PATH=$JAVA_HOME/bin:$PATH
  mvn -Duser.home=/home/jenkins -T 4C clean install -Pdeveloper -DskipTests -Dmaven.javadoc.skip -Dsurefire.forkCount=${ALLUXIO_BUILD_FORKCOUNT} ${mvn_args} $@

  # Revert back to the image default java version to run the test
  JAVA_HOME=${JAVA_HOME_BACKUP}
  PATH=${PATH_BACKUP}
  mvn -Duser.home=/home/jenkins -T 4C test -Pdeveloper -Dmaven.main.skip -Dskip.protoc=true  -Dmaven.javadoc.skip -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dsurefire.forkCount=${ALLUXIO_BUILD_FORKCOUNT} ${mvn_args} $@

  if [ -n "${ALLUXIO_SONAR_ARGS}" ]
  then
    # A separate step to run jacoco report, with all the generated coverage data. This requires the
    # previous 'install' step to generate the jacoco exec data with the 'jacoco' profile.
    #
    # Must exclude some of the modules that fail to run verify again without a clean step. This is ok
    # since these modules do not contain any source code to track for code coverage.
    mvn -T 4C -Dfindbugs.skip -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip -Dlicense.skip -PjacocoReport verify -pl '!webui,!shaded,!shaded/client,!shaded/hadoop'
    # run sonar analysis
    mvn $(echo "${ALLUXIO_SONAR_ARGS}") sonar:sonar
  fi
else
  echo "RUN_MAVEN was not set to true, skipping full maven check and only running license check"
  mvn -Duser.home=/home/jenkins license:check
fi

if [ "${RUN_DOC_CHECK}" == "true" ]; then
  ./dev/scripts/check-docs.sh
else
  echo "RUN_DOC_CHECK was not set to true, skipping doc check"
fi
