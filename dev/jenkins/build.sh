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
  git clean -fdx
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

if [ "$RUN_MAVEN" == "true" ]; then
  # Set things up so that the current user has a real name and can authenticate.
  myuid=$(id -u)
  mygid=$(id -g)
  echo "$myuid:x:$myuid:$mygid:anonymous uid:/home/jenkins:/bin/false" >> /etc/passwd

  export MAVEN_OPTS="-Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat=HH:mm:ss.SSS"

  if [ -z "${ALLUXIO_BUILD_FORKCOUNT}" ]
  then
    ALLUXIO_BUILD_FORKCOUNT=4
  fi

  mvn -Duser.home=/home/jenkins -T 4C clean install -Pdeveloper -Dmaven.javadoc.skip -Dsurefire.forkCount=${ALLUXIO_BUILD_FORKCOUNT} $@

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
  echo "RUN_MAVEN was not set to true, skipping maven check"
fi

if [ "${RUN_DOC_CHECK}" == "true" ]; then
  ./dev/scripts/check-docs.sh
else
  echo "RUN_DOC_CHECK was not set to true, skipping doc check"
fi
