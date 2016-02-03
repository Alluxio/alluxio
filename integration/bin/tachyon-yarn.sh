#!/bin/bash
#
# Usage:
#  tachyon-yarn.sh <numWorkers> <pathHdfs>

function printUsage {
  echo "Usage: tachyon-yarn.sh <numWorkers> <pathHdfs>"
  echo -e "  numWorkers        \tNumber of Tachyon workers to launch"
  echo -e "  pathHdfs          \tPath on HDFS to put tachyon jar and distribute it to YARN"
  echo
  echo "Example: ./tachyon-yarn.sh 10 hdfs://localhost:9000/tmp/"
}

if [ "$#" != 2 ]; then
  printUsage
  exit 1
fi

if [ -z "$HADOOP_HOME" ]; then
  echo "\$HADOOP_HOME is unset, please set this variable to connect to HDFS and YARN";
  exit 1
else
  echo "Using \$HADOOP_HOME set to '$HADOOP_HOME'";
fi

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
TACHYON_HOME="$(cd "${SCRIPT_DIR}/../.."; pwd)"

source "${SCRIPT_DIR}/common.sh"

NUM_WORKERS=$1
HDFS_PATH=$2
TACHYON_TARFILE="tachyon.tar.gz"
rm -rf $TACHYON_TARFILE
tar -C $TACHYON_HOME -zcf $TACHYON_TARFILE \
  assembly/target/tachyon-assemblies-${VERSION}-jar-with-dependencies.jar libexec \
  core/server/src/main/webapp \
  bin conf integration/bin/common.sh integration/bin/tachyon-master-yarn.sh \
  integration/bin/tachyon-worker-yarn.sh \
  integration/bin/tachyon-application-master.sh \

JAR_LOCAL=${TACHYON_HOME}/assembly/target/tachyon-assemblies-${VERSION}-jar-with-dependencies.jar

echo "Uploading files to HDFS to distribute tachyon runtime"

${HADOOP_HOME}/bin/hadoop fs -put -f ${TACHYON_TARFILE} ${HDFS_PATH}/$TACHYON_TARFILE
${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${HDFS_PATH}/tachyon.jar
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/tachyon-yarn-setup.sh ${HDFS_PATH}/tachyon-yarn-setup.sh
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/tachyon-application-master.sh ${HDFS_PATH}/tachyon-application-master.sh

echo "Starting YARN client to launch Tachyon on YARN"

# Add Tachyon java options to the yarn options so that tachyon.yarn.Client can be configured via
# tachyon java options
export YARN_OPTS="${YARN_OPTS:-${TACHYON_JAVA_OPTS}}"

${HADOOP_HOME}/bin/yarn jar ${JAR_LOCAL} tachyon.yarn.Client \
    -num_workers $NUM_WORKERS \
    -master_address ${TACHYON_MASTER_ADDRESS:-localhost} \
    -resource_path ${HDFS_PATH}
