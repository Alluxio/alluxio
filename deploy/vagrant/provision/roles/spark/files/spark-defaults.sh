#!/usr/bin/env bash

cp /spark/conf/spark-defaults.conf.template /spark/conf/spark-defaults.conf

# disk0 should be much larger than root device, put the logs there if it exists
if [[ -d /disk0 ]]; then
  EVENTLOG_DIR=/disk0/spark-eventlog
else
  EVENTLOG_DIR=/tmp/spark-eventlog
fi
mkdir -p ${EVENTLOG_DIR}
cat > /spark/conf/spark-defaults.conf <<EOF
  spark.master spark://AlluxioMaster:7077
  spark.eventLog.enabled true
  spark.eventLog.dir ${EVENTLOG_DIR}
  spark.history.fs.logDirectory ${EVENTLOG_DIR}
  spark.serializer org.apache.spark.serializer.KryoSerializer
  spark.tachyonStore.url alluxio://AlluxioMaster:19998
  # externalBlockStore.url is needed in spark master branch
  spark.externalBlockStore.url alluxio://AlluxioMaster:19998
EOF
