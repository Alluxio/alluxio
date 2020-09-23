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


cp /spark/conf/spark-defaults.conf.template /spark/conf/spark-defaults.conf

# disk0 should be much larger than root device, put the logs there if it exists
if [[ -d /disk0 ]]; then
  EVENTLOG_DIR=/disk0/spark-eventlog
else
  EVENTLOG_DIR=/tmp/spark-eventlog
fi
mkdir -p ${EVENTLOG_DIR}
cat > /spark/conf/spark-defaults.conf << EOF
  spark.master spark://AlluxioMaster:7077
  spark.eventLog.enabled true
  spark.eventLog.dir ${EVENTLOG_DIR}
  spark.history.fs.logDirectory ${EVENTLOG_DIR}
  spark.serializer org.apache.spark.serializer.KryoSerializer
  spark.tachyonStore.url alluxio://AlluxioMaster:19998
  # externalBlockStore.url is needed in spark master branch
  spark.externalBlockStore.url alluxio://AlluxioMaster:19998
EOF
