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

USAGE="Usage: alluxio-checker.sh mapreduce [NUM_MAPS]
where NUM_MAPS is an optional argument which affects the number of map tasks in MapReduce job of integration checker.
  We recommend users to set this number according to your Hadoop cluster size,
  so that MapReduce integration checker can check more Hadoop nodes.
  By default, the NUM_MAPS is 10.

-h  display this help."

ALLUXIO_PATH=$(cd "${BIN}/../../"; pwd)
HADOOP_LOCATION=""
NUM_MAPS="${1:-10}";

# Find the location of hadoop in order to trigger Hadoop job
function find_hadoop_path() {
  [ -f "$(which hadoop)" ] && HADOOP_LOCATION="$(which hadoop)"

  # If ${HADOOP_HOME} has been set
  if [[ "${HADOOP_LOCATION}" == "" ]] && [[ "${HADOOP_HOME}" != "" ]]; then
    {
      [ -f "${HADOOP_HOME}/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}/hadoop"
    } ||   {
      [ -f "${HADOOP_HOME}/bin/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}/bin/hadoop"
    } || {
      [ -f "${HADOOP_HOME}/libexec/bin/hadoop" ] && HADOOP_LOCATION="${HADOOP_HOME}/libexec/bin/hadoop"
    } || { # Try to find hadoop executable file in HADOOP_HOME
      if [[ "${HADOOP_LOCATION}" == "" ]]; then
        array=(`find "${HADOOP_HOME}" -type f -name 'hadoop'`)
        for i in "${array[@]}"; do
          HADOOP_LOCATION="$i"
          break;
        done
      fi
    }
  fi

  if [[ "${HADOOP_LOCATION}" == "" ]]; then
    if [[ "${HADOOP_HOME}" != "" ]]; then
      echo -e "Cannot find executable file hadoop in your HADOOP_HOME: ${HADOOP_HOME}, please check your HADOOP_HOME." >&2
    else
      echo -e "Please set HADOOP_HOME before running MapReduce integration checker." >&2
    fi
    exit 1
  fi
}

function trigger_mapreduce() {
  # Without -libjars, we assume that the Alluxio client jar has already been distributed on the classpath of all Hadoop nodes
  ${LAUNCHER} "${HADOOP_LOCATION}" jar "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" \
    alluxio.checker.MapReduceIntegrationChecker "${NUM_MAPS}"

  # Use -libjars if the previous attempt failed because of unable to find Alluxio classes and add remind information
  if [[ "$?" == 2 ]]; then
    echo "Re-running integration checker with:"  >> "./IntegrationReport.txt"
    echo "    export HADOOP_CLASSPATH=${ALLUXIO_JAR_PATH}:\${HADOOP_CLASSPATH}"  >> "./IntegrationReport.txt"
    echo "    and add -libjar ${ALLUXIO_JAR_PATH} command line option when using hadoop jar command."  >> "./IntegrationReport.txt"

    ${LAUNCHER} export HADOOP_CLASSPATH="${ALLUXIO_JAR_PATH}":${HADOOP_CLASSPATH}
    ${LAUNCHER} "${HADOOP_LOCATION}" jar "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" \
      alluxio.checker.MapReduceIntegrationChecker -libjars "${ALLUXIO_JAR_PATH}" "${NUM_MAPS}"

    if [[ "$?" == 0 ]]; then
      echo "Please use the -libjars ${ALLUXIO_JAR_PATH} command line option when using hadoop jar." \
        >> "./IntegrationReport.txt"
      echo "" >> "./IntegrationReport.txt"
      echo "Please export HADOOP_CLASSPATH=${ALLUXIO_JAR_PATH}:\${HADOOP_CLASSPATH} to make Alluxio client jar available to client JVM created by hadoop jar command " \
        >> "./IntegrationReport.txt"
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

  # Check if too many arguments has been passed in
  if [ "$#" -gt 1 ]; then
    echo -e "${USAGE}" >&2
    echo "Too many arguments passed in"
    exit 1
  fi

  # Check if NUM_MAPS is valid
  if [[ -n ${NUM_MAPS//[0-9]/} ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi

  source "${ALLUXIO_PATH}/libexec/alluxio-config.sh"
  ALLUXIO_JAR_PATH="${ALLUXIO_PATH}/client/alluxio-${VERSION}-client.jar"

  [ -f "./IntegrationReport.txt" ] && rm "./IntegrationReport.txt"
  find_hadoop_path
  trigger_mapreduce
  [ -f "./IntegrationReport.txt" ] && cat "./IntegrationReport.txt" && rm "./IntegrationReport.txt"
}

main "$@"
