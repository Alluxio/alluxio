#!/bin/bash
#
# Usage:
#  alluxio-yarn.sh <numWorkers> <pathHdfs>

function printUsage {
  echo "Usage: alluxio-yarn.sh <numWorkers> <pathHdfs>"
  echo -e "  numWorkers        \tNumber of Alluxio workers to launch"
  echo -e "  pathHdfs          \tPath on HDFS to put alluxio jar and distribute it to YARN"
  echo
  echo "Example: ./alluxio-yarn.sh 10 hdfs://localhost:9000/tmp/"
}

if [[ "$#" != 2 ]]; then
  printUsage
  exit 1
fi

if [[ -z "$HADOOP_HOME" ]]; then
  echo "\$HADOOP_HOME is unset, please set this variable to connect to HDFS and YARN";
  exit 1
else
  echo "Using \$HADOOP_HOME set to '$HADOOP_HOME'";
fi

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
ALLUXIO_HOME="$(cd "${SCRIPT_DIR}/../.."; pwd)"

source "${SCRIPT_DIR}/common.sh"

NUM_WORKERS=$1
HDFS_PATH=$2
ALLUXIO_TARFILE="alluxio.tar.gz"
rm -rf $ALLUXIO_TARFILE
tar -C $ALLUXIO_HOME -zcf $ALLUXIO_TARFILE \
  assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar libexec \
  core/server/src/main/webapp \
  bin conf integration/bin/common.sh integration/bin/alluxio-master-yarn.sh \
  integration/bin/alluxio-worker-yarn.sh \
  integration/bin/alluxio-application-master.sh \

JAR_LOCAL=${ALLUXIO_HOME}/assembly/target/alluxio-assemblies-${VERSION}-jar-with-dependencies.jar

echo "Uploading files to HDFS to distribute alluxio runtime"

${HADOOP_HOME}/bin/hadoop fs -put -f ${ALLUXIO_TARFILE} ${HDFS_PATH}/$ALLUXIO_TARFILE
${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${HDFS_PATH}/alluxio.jar
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/alluxio-yarn-setup.sh ${HDFS_PATH}/alluxio-yarn-setup.sh
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/alluxio-application-master.sh ${HDFS_PATH}/alluxio-application-master.sh

echo "Starting YARN client to launch Alluxio on YARN"

# Add Alluxio java options to the yarn options so that alluxio.yarn.Client can be configured via
# alluxio java options
export YARN_OPTS="${YARN_OPTS:-${ALLUXIO_JAVA_OPTS}}"

${HADOOP_HOME}/bin/yarn jar ${JAR_LOCAL} alluxio.yarn.Client \
    -num_workers $NUM_WORKERS \
    -master_address ${ALLUXIO_MASTER_ADDRESS:-localhost} \
    -resource_path ${HDFS_PATH}
