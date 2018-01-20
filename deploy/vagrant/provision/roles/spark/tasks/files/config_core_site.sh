#!/usr/bin/env bash

cat > /spark/conf/core-site.xml << EOF
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
EOF
