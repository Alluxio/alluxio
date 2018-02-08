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

USAGE="Usage: alluxio-checker.sh mapreduce [INPUT_SPLITS]
where INPUT_SPLITS is an optional argument which defines the number of input files
  and affects the number of map tasks in MapReduce job of integration checker.
  We recommend users to set this number to the Hadoop cluster size,
  so that MapReduce integration checker can check more Hadoop nodes.
  By default, the INPUT_SPLITS is 10.

-h  display this help."

HADOOP_LOCATION=""
INPUT_SPLITS="${1:-10}";

# Find the location of hadoop in order to trigger Hadoop job
function find_hadoop_path() {
  { # Try HADOOP_HOME
    [ -f "${HADOOP_HOME}/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}"
  } ||   {
    [ -f "${HADOOP_HOME}/bin/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}/bin"
  } || {
    [ -f "${HADOOP_HOME}/libexec/bin/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}/libexec/bin"
  } || { # Try PATH
    IFS=':' read -ra PATHARR <<< "$PATH"
    for p in "${PATHARR[@]}"; do
      if [ -f "$p/hadoop" ]; then
        HADOOP_LOCATION="$p"
        break;
      elif [ -f "$p/bin/hadoop" ]; then
        HADOOP_LOCATION="$p/bin"
        break;
      elif [ -f "$p/libexec/bin/hadoop" ]; then
        HADOOP_LOCATION="$p/libexec/bin"
        break;
      fi
    done
    if [[ "${HADOOP_LOCATION}" == "" ]]; then
      echo -e "Please set HADOOP_HOME before running MapReduce integration checker." >&2
      exit 1
    fi
  }
}

function trigger_mapreduce() {
  # Without -libjars, we assumes that the Alluxio client jar has already distributed on the classpath of all Hadoop nodes
  ${LAUNCHER} "${HADOOP_LOCATION}/hadoop" jar "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" \
    alluxio.checker.MapReduceIntegrationChecker "${INPUT_SPLITS}"

  # Use -libjars if the previous attempt failed and add remind information
  if [[ "$?" != 0 ]]; then
    ${LAUNCHER} "${HADOOP_LOCATION}/hadoop" jar "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" \
      alluxio.checker.MapReduceIntegrationChecker -libjars "${BIN}/../../client/alluxio-${VERSION}-client.jar" "${INPUT_SPLITS}"

    echo "Please use the -libjars command line option when using hadoop jar ..., specifying /<PATH_TO_ALLUXIO>/client/alluxio-${VERSION}-client.jar as the argument of -libjars." \
      >> "./MapReduceIntegrationReport.txt"
  fi
}

function main {
  # Check if INPUT_SPLITS is valid
  if [[ -n ${INPUT_SPLITS//[0-9]/} ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi
  source "${BIN}/../../libexec/alluxio-config.sh"
  [ -f "./MapReduceIntegrationReport.txt" ] && rm "./MapReduceIntegrationReport.txt"
  find_hadoop_path
  trigger_mapreduce
  [ -f "./MapReduceIntegrationReport.txt" ] && cat "./MapReduceIntegrationReport.txt" && rm "./MapReduceIntegrationReport.txt"
}

main "$@"
