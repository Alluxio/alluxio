#!/bin/bash
#
# Usage:
#  tachyon-yarn-submit.sh <numWorkers> <pathHdfs>

function printUsage {
  echo "Usage: tachyon-yarn-submit.sh <numWorkers> <pathHdfs>"
  echo -e "  numWorkers        \tNumber of Tachyon workers to launch"
  echo -e "  pathHdfs          \tpath on HDFS to put tachyon jar and distribute it to YARN"
  echo
  echo "Example: ./tachyon-yarn-submit.sh 10 hdfs://localhost:9000/tmp/"
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
  servers/src/main/webapp \
  bin conf integration/bin/common.sh integration/bin/tachyon-master-yarn.sh \
  integration/bin/tachyon-worker-yarn.sh

JAR_LOCAL=${TACHYON_HOME}/assembly/target/tachyon-assemblies-${VERSION}-jar-with-dependencies.jar


${HADOOP_HOME}/bin/hadoop fs -put -f ${TACHYON_TARFILE} ${HDFS_PATH}/$TACHYON_TARFILE
${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${HDFS_PATH}/tachyon.jar
${HADOOP_HOME}/bin/hadoop fs -put -f ./tachyon-yarn-setup.sh ${HDFS_PATH}/tachyon-yarn-setup.sh


${HADOOP_HOME}/bin/yarn jar ${JAR_LOCAL} tachyon.yarn.Client \
    -num_workers $NUM_WORKERS \
    -master_address localhost \
    -resource_path ${HDFS_PATH}

exit 0


${HADOOP_HOME}/bin/hadoop fs -put -f ./tachyon-master-yarn.sh ${HDFS_PATH}/tachyon-master-yarn.sh
${HADOOP_HOME}/bin/hadoop fs -put -f ./tachyon-worker-yarn.sh ${HDFS_PATH}/tachyon-worker-yarn.sh

${HADOOP_HOME}/bin/yarn jar ${JAR_LOCAL} tachyon.yarn.Client \
    -num_workers $NUM_WORKERS \
    -master_address localhost \
    -resource_path ${HDFS_PATH} \
    -master_java_opts "$TACHYON_MASTER_JAVA_OPTS" \
    -worker_java_opts "$TACHYON_WORKER_JAVA_OPTS"
