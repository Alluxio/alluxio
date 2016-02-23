#!/usr/bin/env bash

echo "export HADOOP_CONF_DIR=/hadoop/conf" >> /spark/conf/spark-env.sh

cat > /spark/conf/core-site.xml << EOF
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
EOF
