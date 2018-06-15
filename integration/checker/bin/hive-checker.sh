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

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
CHECKER_BIN_PATH=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: alluxio-checker.sh hive [-mode USER_MODE] [-hiveurl HIVE_URL] [-name HIVE_USER_NAME] [-password HIVE_USER_PASSWORD]

Prerequisites:
    Hive cluster and Alluxio cluster are running.
    hive-server2 is running.
    alluxio.master.hostname is configured in your <ALLUXIO_HOME>/conf/alluxio-site.properties.

Argument:
    -hiveurl HIVE_URL is a Hive url in the form jdbc:subprotocol:subname.

Optional arguments:
    -mode USER_MODE is one of the following value, by default the value is location.
          dfs                    Alluxio is configured as Hive default filesystem.
          location               Alluxio is used as a location of Hive tables other than Hive default filesystem.
    -user HIVE_USER_NAME         is the Hive user on whose behalf the connection is being made, by default is the system username.
    -password HIVE_USER_PASSWORD is the Hive user's password, by default is an empty string.

-h  display this help."

PERMISSION_DENIED_MESSAGE="
Create folder failed because of Alluxio permission denied.

Please use \"<ALLUXIO_HOME>/bin/alluxio fs mkdir /alluxioTestFolder\" to create the test folder
and use \"<ALLUXIO_HOME>/bin/alluxio fs chmod 777 /alluxioTestFolder\" to change the folder permission.

Please rerun the Hive integration checker."

ALLUXIO_PATH=$(cd "${CHECKER_BIN_PATH}/../../../"; pwd)

function generate_input() {
  [ -f "./IntegrationReport.txt" ] && rm "./IntegrationReport.txt"

  if [[ "${HIVE_USER_MODE}" == "location" ]]; then
    ${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs test -e /alluxioTestFolder > /dev/null
    if [[ "$?" != 0 ]]; then
      CREATE_FOLDER_MESSAGE=$(${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs mkdir /alluxioTestFolder)
      if [[ "$?" != 0 ]]; then
        echo "Generating Alluxio test folder."
        echo "Running <ALLUXIO_HOME>/bin/alluxio fs mkdir /alluxioTestFolder"
        echo "${CREATE_FOLDER_MESSAGE}"
        if [[ "${CREATE_FOLDER_MESSAGE}" == Permission* ]]; then
          echo -e "${PERMISSION_DENIED_MESSAGE}" >&2
          exit 1
        fi
      fi
    fi
  fi
}

function trigger_hive() {
  echo "Hive integration checker is running."
  if [[ "${ALLUXIO_URL}" == "" ]]; then
    ${LAUNCHER} java -cp "${ALLUXIO_CHECKER_JAR}" alluxio.checker.HiveIntegrationChecker "$@"
  else
    ${LAUNCHER} java -cp "${ALLUXIO_CHECKER_JAR}" alluxio.checker.HiveIntegrationChecker "$@" -alluxioUrl "${ALLUXIO_URL}"
  fi
}

function clean_output() {
  [ -f "./IntegrationReport.txt" ] && rm "./IntegrationReport.txt"
  if [[ "${HIVE_USER_MODE}" == "location" ]]; then
    DELETE_FOLDER_MESSAGE=$(${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs rm -R /alluxioTestFolder)
    if [[ "$?" != 0 ]]; then
    echo "Removing Alluxio test folder."
      echo "Running <ALLUXIO_HOME>/bin/alluxio fs rm -R /alluxioTestFolder"
      echo "${DELETE_FOLDER_MESSAGE}"
    fi
  fi
}

function main {
  # Check if an input argument is -h
  for i in "$@"; do
    if [[ "$i" == "-h" ]]; then
      echo -e "${USAGE}" >&2
      exit 0
    fi
  done

  # Find out the HIVE_USER_MODE value
  for (( i=1; i<="$#"; i++)); do
    j="$((i+1))"
    if [[ "${!i}" == -mode ]]; then
      HIVE_USER_MODE="${!j}"
    fi
  done

  if [[ "${HIVE_USER_MODE}" == "" ]]; then
    HIVE_USER_MODE="location"
  fi

  source "${ALLUXIO_PATH}/libexec/alluxio-config.sh"
  ALLUXIO_CHECKER_JAR="${ALLUXIO_PATH}/checker/target/alluxio-checker-${VERSION}-jar-with-dependencies.jar"
  ALLUXIO_BIN_PATH="${ALLUXIO_PATH}/bin/alluxio"

  if [[ "${HIVE_USER_MODE}" == "location" ]]; then
    ALLUXIO_MASTER_HOSTNAME=$(${LAUNCHER} "${ALLUXIO_BIN_PATH}" getConf alluxio.master.hostname)
    ALLUXIO_MASTER_PORT=$(${LAUNCHER} "${ALLUXIO_BIN_PATH}" getConf alluxio.master.port)
    ALLUXIO_URL="alluxio://${ALLUXIO_MASTER_HOSTNAME}:${ALLUXIO_MASTER_PORT}"
    if [[ "${ALLUXIO_MASTER_HOSTNAME}" == "" ]] || [[ "${ALLUXIO_MASTER_PORT}" == "" ]]; then
      echo -e "${USAGE}" >&2
      echo "Please set the alluxio.master.hostname and alluxio.master.port in your ${ALLUXIO_PATH}/conf/alluxio-site.properties."
      exit 1
    fi
  fi

  generate_input
  trigger_hive "$@"

  # View the checker result
  [ -f "./IntegrationReport.txt" ] && cat "./IntegrationReport.txt"

  clean_output
}

main "$@"
