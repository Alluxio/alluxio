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


# let mapreduce be able to run against Alluxio
ALLUXIO_CLIENT_JAR=$(ls /alluxio/core/client/target/alluxio-core-client-*-jar-with-dependencies.jar)
echo "export HADOOP_CLASSPATH=\${HADOOP_CLASSPATH}:${ALLUXIO_CLIENT_JAR}" >> /hadoop/conf/hadoop-env.sh

NODES=$(cat /vagrant/files/workers)

# setup hadoop
rm -f /hadoop/conf/slaves
for node in ${NODES[@]}; do
  echo ${node} >> /hadoop/conf/slaves
done

# choose the last node as namenode
namenode=${node}
echo ${namenode} > /hadoop/conf/masters

# use /disk0, /disk1... as local storage
EXTRA_DISKS=$(ls / | grep '^disk')
DN=""
NN=""
TMP=""
for disk in ${EXTRA_DISKS}; do
  DN=/${disk}/dfs/dn,${DN}
  NN=/${disk}/dfs/nn,${NN}
  TMP=/${disk}/hadoop-tmpstore,${TMP}
done

[[ "$TMP" == "" ]] && TMP=/tmp/hadoop-tmpstore
cat > /hadoop/conf/core-site.xml << EOF
<configuration>
<property>
  <name>hadoop.tmp.dir</name>
  <value>${TMP}</value>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://${namenode}:9000</value>
</property>
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
</configuration>
EOF

[[ "$DN" == "" ]] && DN=/tmp/hdfs-datanode
[[ "$NN" == "" ]] && NN=/tmp/hdfs-namenode
cat > /hadoop/conf/hdfs-site.xml << EOF
<configuration>
<property>
 <name>dfs.replication</name>
 <value>1</value>
</property>
<property>
 <name>dfs.data.dir</name>
 <value>${DN}</value>
</property>
<property>
 <name>dfs.name.dir</name>
 <value>${NN}</value>
</property>
<property>
 <name>dfs.support.broken.append</name>
 <value>true</value>
</property>
<property>
 <name>dfs.webhdfs.enabled</name>
 <value>true</value>
</property>
</configuration>
EOF

cat > /hadoop/conf/mapred-site.xml << EOF
<configuration>
<property>
 <name>mapred.job.tracker</name>
 <value>${namenode}:9001</value>
</property>
</configuration>
EOF
