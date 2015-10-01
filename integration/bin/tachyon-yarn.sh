#!/bin/bash
#
# Usage:
#  tachyon-yarn.sh <deployTachyonHome> <numWorkers> <jarLocal> <jarHdfs>

function printUsage {
  echo "Usage: tachyon-yarn.sh <deployTachyonHome> <numWorkers> <jarLocal> <jarHdfs>"
  echo -e "  deployTachyonHome \tHome directory of Tachyon deployment on YARN slave machines"
  echo -e "  numWorkers        \tNumber of Tachyon workers to launch"
  echo -e "  jarLocal          \tpath on local filesystem to tachyon jar"
  echo -e "  jarHdfs           \tpath on HDFS to put tachyon jar and distribute it to YARN"
  echo
  echo "Example: integration/bin/tachyon-yarn.sh /path/to/tachyon/deployment 10 \\"
  echo "    assembly/target/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar \\"
  echo "    hdfs://NAMENODE:9000/tmp/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar"
}

if [ "$#" != 4 ]; then
  printUsage
  exit 1
fi

if [ -z "$HADOOP_HOME" ]; then
  echo "\$HADOOP_HOME is unset, please set this variable to connect to HDFS and YARN";
  exit 1
else
  echo "Using \$HADOOP_HOME set to '$HADOOP_HOME'";
fi


DEPLOY_TACHYON_HOME=$1
NUM_WORKERS=$2
JAR_LOCAL=$3
JAR_HDFS=$4

${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${JAR_HDFS}

${HADOOP_HOME}/bin/yarn jar ${JAR_LOCAL} tachyon.yarn.Client -jar ${JAR_HDFS} \
    -num_workers $NUM_WORKERS -tachyon_home ${DEPLOY_TACHYON_HOME} -master_address localhost
