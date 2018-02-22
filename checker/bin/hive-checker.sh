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
    Please use ${HIVE_HOME}/bin/hiveserver2 to start hiveserver2.
    Please set the alluxio.master.hostname in your <ALLUXIO_HOME>/conf/alluxio-site.properties.

Argument:
    -hiveurl HIVE_URL is a database url of form jdbc:subprotocol:subname.

Optional arguments:
    -mode USER_MODE is one of the following integer, by default the value is 1.
        1   use Alluxio as one option to store Hive tables.
        2   use Alluxio as the Hive default filesystem.
    -user HIVE_USER_NAME is the database user on whose behalf the connection is being made, by default is the system username.
    -password HIVE_USER_PASSWORD is the user's password, if it is an empty string, you do not need to pass in anything.

-h  display this help."

ALLUXIO_BIN_PATH=""
ALLUXIO_CHECKER_JAR=""
ALLUXIO_PATH=$(cd "${CHECKER_BIN_PATH}/../../"; pwd)
ALLUXIO_URL=""
HIVE_USER_MODE=""

function generate_input() {
  [ -f "./IntegrationReport.txt" ] && rm "./IntegrationReport.txt"

  if [[ "${HIVE_USER_MODE}" == "1" ]]; then
    # Generate the input file for Hive integration checker
    echo "You|Pass" > ~/hiveTestTable
    echo "Hive|Test" >> ~/hiveTestTable
    # If we want to use Alluxio as one option to store hive tables, we need input file exists in the Alluxio filesystem
    ${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs mkdir /alluxioTestFolder >/dev/null
    ${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs copyFromLocal ~/hiveTestTable /alluxioTestFolder/ >/dev/null
  fi
}

function trigger_hive() {
  ${LAUNCHER} java -cp "${ALLUXIO_CHECKER_JAR}" alluxio.checker.HiveIntegrationChecker "$@" -alluxiourl "${ALLUXIO_URL}"
  # If input is not valid, we print helpful message
  if [[ "$?" == "2" ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi
}

function clean_output() {
  [ -f "./IntegrationReport.txt" ] && cat "./IntegrationReport.txt" && rm "./IntegrationReport.txt"
  if [[ "${HIVE_USER_MODE}" == "1" ]]; then
    ${LAUNCHER} "${ALLUXIO_BIN_PATH}" fs rm -R "${ALLUXIO_URL}/alluxioTestFolder" >/dev/null
  fi
  [ -f "~/hiveTestTable" ] && rm "~/hiveTestTable"
}

function main {
  # Check if an input argument is -h
  for i in "$@"; do
    if [[ "$i" == "-h" ]]; then
      echo -e "${USAGE}" >&2
      exit 0
    fi
  done

  for i in "$@"; do
    if [[ "$i" == "-mode" ]]; then
      HIVE_USER_MODE="${i+1}"
    fi
  done

  if [[ "${HIVE_USER_MODE}" == "" ]]; then
    HIVE_USER_MODE=1
  fi

  source "${ALLUXIO_PATH}/libexec/alluxio-config.sh"
  ALLUXIO_CHECKER_JAR="${ALLUXIO_PATH}/checker/target/alluxio-checker-${VERSION}-jar-with-dependencies.jar"
  ALLUXIO_BIN_PATH="${ALLUXIO_PATH}/bin/alluxio"

  if [[ "${HIVE_USER_MODE}" == "1" ]]; then
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
  clean_output

}

main "$@"
