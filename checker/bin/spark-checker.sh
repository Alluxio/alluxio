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

USAGE="Usage: alluxio-checker.sh Spark [SPARK_MASTER_ADDRESS] [PARTITIONS]
The SPARK_MASTER_ADDRESS should be one of the following:
  local                 \ Running Spark on local machine.
  spark://host:port     \ Spark standalone mode.
  mesos://host:port     \ Running Spark on Mesos.
  yarn                  \ launching Spark on Yarn.

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
  { # Try SPARK_HOME
    [ -f "${SPARK_HOME}/bin/spark-submit" ] && SPARK_SUBMIT="${SPARK_HOME}/bin"
  } || { # Try SPARKPATH
    [ -f "${SPARKPATH}/spark-submit" ] && SPARK_SUBMIT="${SPARKPATH}"
  } || { # Try PATH
    IFS=':' read -ra PATHARR <<< "$PATH"
    for p in "${PATHARR[@]}"; do
      if [ -f "$p/spark-submit" ]; then
        SPARK_SUBMIT="$p"
        break;
      fi
    done
    if [[ "${SPARK_SUBMIT}" == "" ]]; then
      echo -e "Please set SPARK_HOME or SPARKPATH before running Spark integration checker." >&2
      exit 1
    fi
  } 
}

function trigger_spark_cluster() {
  # Client mode
  ${LAUNCHER} "${SPARK_SUBMIT}/spark-submit" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    --deploy-mode client "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" --partitions "${PARTITIONS}"

  # Cluster mode
  ${LAUNCHER} "${SPARK_SUBMIT}/spark-submit" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    --deploy-mode cluster "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" --partitions "${PARTITIONS}"
}

function trigger_spark_local() {
  ${LAUNCHER} "${SPARK_SUBMIT}/spark-submit" --class alluxio.checker.SparkIntegrationChecker --master "${SPARK_MASTER}" \
    "${BIN}/../target/alluxio-checker-${VERSION}-jar-with-dependencies.jar" --partitions "${PARTITIONS}"
}

function main {
  source "${BIN}/../../libexec/alluxio-config.sh"
  [ -f "./SparkOutputFile.txt" ] && rm "./SparkOutputFile.txt"
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
  [ -f "./SparkOutputFile.txt" ] && cat "./SparkOutputFile.txt" && rm "./SparkOutputFile.txt"
}

main "$@"
