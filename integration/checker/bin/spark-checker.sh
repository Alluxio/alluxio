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

USAGE="Usage: alluxio-checker.sh spark [SPARK_MASTER_ADDRESS] [PARTITIONS]
The SPARK_MASTER_ADDRESS should be one of the following:
  local or local[*] or local[K,F]  Running Spark on local machine.
  spark://host:port                Spark standalone mode.
  mesos://host:port                Running Spark on Mesos.
  yarn                             Launching Spark on Yarn.

PARTITIONS
  optional Spark argument.
  The partition number that allows Spark to distribute dataset better.
  Please set the partition number according to your Spark cluster size
  so that integration checker can check more Spark executors.
  By default, the value is 10.

-h  display this help."

PARTITIONS="${2:-10}";
SPARK_MASTER="$1";
SPARK_SUBMIT="";

# Find the location of spark-submit in order to run the Spark job
function find_spark_path() {
  [ -f "$(which spark-submit)" ] && SPARK_SUBMIT="$(which spark-submit)"

  # Try to find spark_submit in ${SPARK_HOME}
  if [[ "${SPARK_SUBMIT}" == "" ]] && [[ "${SPARK_HOME}" != "" ]]; then
    {
      [ -f "${SPARK_HOME}/bin/spark-submit" ] && SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
    } || {
      if [[ "${SPARK_SUBMIT}" == "" ]]; then
        array=(`find "${SPARK_HOME}" -type f -name 'spark-submit'`)
        for i in "${array[@]}"; do
          SPARK_SUBMIT="$i"
          break;
        done
      fi
    }
  fi

  # Try to find spark_submit in ${SPARKPATH}
  if [[ "${SPARK_SUBMIT}" == "" ]] && [[ "${SPARKPATH}" != "" ]]; then
    {
      [ -f "${SPARKPATH}/spark-submit" ] && SPARK_SUBMIT="${SPARKPATH}/spark-submit"
    } || {
      if [[ "${SPARK_SUBMIT}" == "" ]]; then
        array=(`find "${SPARKPATH}" -type f -name 'spark-submit'`)
        for i in "${array[@]}"; do
          SPARK_SUBMIT="$i"
          break;
        done
      fi
    }
  fi

  if [[ "${SPARK_SUBMIT}" == "" ]]; then
    if [[ "${SPARK_HOME}" != "" ]]; then
      echo -e "Cannot find spark-submit in your SPARK_HOME: ${SPARK_HOME}, please check your SPARK_HOME." >&2
    else
      echo -e "Please set SPARK_HOME before running Spark integration checker." >&2
    fi
    exit 1
  fi
}

function trigger_spark_cluster() {
  # Client mode
  ${LAUNCHER} "${SPARK_SUBMIT}" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    --deploy-mode client "${ALLUXIO_CHECKER_JAR}" --partitions "${PARTITIONS}"

  # Cluster mode
  ${LAUNCHER} "${SPARK_SUBMIT}" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    --deploy-mode cluster "${ALLUXIO_CHECKER_JAR}" --partitions "${PARTITIONS}"
}

function trigger_spark_local() {
  ${LAUNCHER} "${SPARK_SUBMIT}" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    "${ALLUXIO_CHECKER_JAR}" --partitions "${PARTITIONS}"
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
  if [ "$#" -gt 2 ]; then
    echo -e "${USAGE}" >&2
    echo "Too many arguments passed in."
    exit 1
  fi

  source "${BIN}/../../../libexec/alluxio-config.sh"
  ALLUXIO_CHECKER_JAR="${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar"

  [ -f "./IntegrationReport.txt" ] && rm "./IntegrationReport.txt"
  case "${SPARK_MASTER}" in
    local*)
      find_spark_path
      trigger_spark_local
      ;;
    mesos://* | spark://* | yarn)
      find_spark_path
      trigger_spark_cluster
      ;;
    *)
      echo -e "${USAGE}" >&2
      exit 1
  esac
  [ -f "./IntegrationReport.txt" ] && cat "./IntegrationReport.txt" && rm "./IntegrationReport.txt"
}

main "$@"
