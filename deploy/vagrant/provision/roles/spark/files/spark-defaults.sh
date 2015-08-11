#!/usr/bin/env bash

cp /spark/conf/spark-defaults.conf.template /spark/conf/spark-defaults.conf
mkdir -p /tmp/spark-eventlog
cat > /spark/conf/spark-defaults.conf <<EOF
  spark.master spark://TachyonMaster:7077
  spark.eventLog.enabled true
  spark.eventLog.dir /tmp/spark-eventlog
  spark.serializer org.apache.spark.serializer.KryoSerializer
  spark.tachyonStore.url tachyon://TachyonMaster:19998
  # externalBlockStore.url is needed in spark master branch
  spark.externalBlockStore.url tachyon://TachyonMaster:19998
EOF
