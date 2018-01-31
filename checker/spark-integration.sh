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
CHECKER=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: checkIntegration Spark [SPARK_MASTER_ADDRESS]
The SPARK_MASTER_ADDRESS should be one of the following:
  local                 \ Run Spark on local machine
  spark://host:port     \ Spark standalone mode
  mesos://host:port     \ Running Spark on Mesos
  yarn                  \ launching Spark on Yarn
-h  display this help."

function trigger_spark_local () {
  ${LAUNCHER} "$SPARK_HOME/bin/spark-submit" --class alluxio.checker.SparkIntegrationChecker \
  --master $1 "${CHECKER}/target/alluxio-checker-1.8.0-SNAPSHOT.jar"
}

function trigger_spark_cluster() {
  #client mode
  ${LAUNCHER} "$SPARK_HOME/bin/spark-submit" --class alluxio.checker.SparkIntegrationChecker \
  --master $1  --deploy-mode client "${CHECKER}/target/alluxio-checker-1.8.0-SNAPSHOT.jar"

  #cluster mode
  ${LAUNCHER} "$SPARK_HOME/bin/spark-submit" --class alluxio.checker.SparkIntegrationChecker \
  --master $1 --deploy-mode cluster "${CHECKER}/target/alluxio-checker-1.8.0-SNAPSHOT.jar"
}

function main {
  case "$1" in
    local*)
      trigger_spark_local $1
      ;;
    spark://* | mesos://* | yarn)
      trigger_spark_cluster $1
      ;;
    *)
    echo -e "${USAGE}" >&2
    exit 1
  esac
}

main "$@"