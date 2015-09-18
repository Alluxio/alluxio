#!/bin/bash
#
# Usage:
#  tachyon-yarn.sh

APP_MASTER_JAR=hdfs://localhost:9000/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar

${HADOOP_HOME}/bin/hadoop fs -put -f ${TACHYON_HOME}/assembly/target/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar ${APP_MASTER_JAR}

${HADOOP_HOME}/bin/yarn jar ${TACHYON_HOME}/assembly/target/tachyon-assemblies-0.8.0-SNAPSHOT-jar-with-dependencies.jar tachyon.yarn.Client -jar ${APP_MASTER_JAR} \
    -num_workers 1 -tachyon_home ${TACHYON_HOME} -master_address localhost
