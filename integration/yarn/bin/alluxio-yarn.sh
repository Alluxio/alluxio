#!/bin/bash
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
# Usage:
#  alluxio-yarn.sh <numWorkers> <pathHdfs>

function printUsage {
  echo "Usage: alluxio-yarn.sh <numWorkers> <pathHdfs> [masterAddress]"
  echo -e "  numWorkers        \tNumber of Alluxio workers to launch"
  echo -e "  pathHdfs          \tPath on HDFS to put alluxio jar and distribute it to YARN"
  echo -e "  masterAddress     \tYarn node to launch the Alluxio master on, defaults to ALLUXIO_MASTER_HOSTNAME"
  echo -e "                    \tUsing \"any\" if the master can be launched on any host of YARN"
  echo
  echo "Example: ./alluxio-yarn.sh 10 hdfs://localhost:9000/tmp/ ip-172-31-5-205.ec2.internal"
  echo "Example: ./alluxio-yarn.sh 10 hdfs://localhost:9000/tmp/ any"
}

if [[ "$#" -lt 2 ]] || [[ "$#" -gt 3 ]]; then
  printUsage
  exit 1
fi

if [[ -z "$HADOOP_HOME" ]]; then
  echo "\$HADOOP_HOME is unset, please set this variable to connect to HDFS and YARN" >&2
  exit 1
else
  echo "Using \$HADOOP_HOME set to '$HADOOP_HOME'"
fi

if [[ -z "$YARN_HOME" ]]; then
  echo "\$YARN_HOME is unset, will use \$HADOOP_HOME instead."
fi
YARN_HOME=${YARN_HOME:-${HADOOP_HOME}}

SCRIPT_DIR="$(cd "$(dirname "$(readlink "$0" || echo "$0")")"; pwd)"
ALLUXIO_HOME="$(cd "${SCRIPT_DIR}/../../.."; pwd)"

source "${SCRIPT_DIR}/common.sh"

NUM_WORKERS=$1
HDFS_PATH=$2
MASTER_ADDRESS=${3:-${ALLUXIO_MASTER_HOSTNAME}}

ALLUXIO_TARFILE="alluxio.tar.gz"
rm -rf $ALLUXIO_TARFILE
tar -C $ALLUXIO_HOME -zcf $ALLUXIO_TARFILE \
  assembly/alluxio-server-${VERSION}.jar \
  integration/yarn/bin/alluxio-application-master.sh \
  integration/yarn/bin/alluxio-master-yarn.sh \
  integration/yarn/bin/alluxio-worker-yarn.sh \
  integration/yarn/bin/common.sh \
  bin conf lib libexec

JAR_LOCAL=${ALLUXIO_HOME}/integration/yarn/alluxio-yarn-${VERSION}.jar

echo "Uploading files to HDFS to distribute alluxio runtime"

${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${HDFS_PATH}
${HADOOP_HOME}/bin/hadoop fs -put -f ${ALLUXIO_TARFILE} ${HDFS_PATH}/${ALLUXIO_TARFILE}
${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${HDFS_PATH}/alluxio.jar
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/alluxio-yarn-setup.sh ${HDFS_PATH}/alluxio-yarn-setup.sh
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/alluxio-application-master.sh ${HDFS_PATH}/alluxio-application-master.sh

echo "Starting YARN client to launch Alluxio on YARN"

# Add Alluxio java options to the yarn options so that alluxio.yarn.Client can be configured via
# alluxio java options
ALLUXIO_JAVA_OPTS="${ALLUXIO_JAVA_OPTS} -Dalluxio.logger.type=Console"
export YARN_OPTS="${YARN_OPTS} ${ALLUXIO_JAVA_OPTS}"

${YARN_HOME}/bin/yarn jar ${JAR_LOCAL} alluxio.yarn.Client \
    -num_workers ${NUM_WORKERS} \
    -master_address ${MASTER_ADDRESS} \
    -resource_path ${HDFS_PATH}
