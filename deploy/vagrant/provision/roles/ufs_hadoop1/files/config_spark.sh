#!/usr/bin/env bash

echo "export HADOOP_CONF_DIR=/hadoop/conf" >> /spark/conf/spark-env.sh

cat > /spark/conf/core-site.xml << EOF
<configuration>
  <property>
    <name>fs.tachyon.impl</name>
    <value>tachyon.hadoop.TFS</value>
  </property>
</configuration>
EOF
