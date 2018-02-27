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
BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: alluxio-checker.sh FRAMEWORK [FRAMEWORK_ARGS]
Where FRAMEWORK is one of the following:
    spark
    mapreduce
    hive

-h  display this help.
FRAMEWORK -h display the FRAMEWORK detailed help."

function run_spark() {
  ${LAUNCHER} "${BIN}/spark-checker.sh" "$@"
}

function run_mapreduce() {
  ${LAUNCHER} "${BIN}/mapreduce-checker.sh" "$@"
}

function run_hive() {
  ${LAUNCHER} "${BIN}/hive-checker.sh" "$@"
}

function main {
  FRAMEWORK=$(echo "$1" | tr '[:upper:]' '[:lower:]')
  shift
  case "${FRAMEWORK}" in
    spark)
      run_spark "$@"
      ;;
    mapreduce)
      run_mapreduce "$@"
      ;;
    hive)
      run_hive "$@"
      ;;
    *)
      echo -e "${USAGE}" >&2
      exit 1
  esac
}

main "$@"
