#!/usr/bin/env bash

# let mapreduce be able to run against Alluxio
ALLUXIO_CLIENT_JAR=$(ls /alluxio/core/client/target/alluxio-core-client-*-jar-with-dependencies.jar)
echo "export HADOOP_CLASSPATH=\${HADOOP_CLASSPATH}:${ALLUXIO_CLIENT_JAR}" >> /hadoop/etc/hadoop/hadoop-env.sh

NODES=`cat /vagrant/files/workers`

# setup hadoop
rm -f /hadoop/etc/hadoop/slaves
for i in ${NODES[@]}; do
 echo $i >> /hadoop/etc/hadoop/slaves
done

# choose the last node as namenode
namenode=$i
cat > /hadoop/etc/hadoop/core-site.xml << EOF
<configuration>
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://${namenode}:9000</value>
</property>
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
</property>
</configuration>
EOF

cat > /hadoop/etc/hadoop/hdfs-site.xml << EOF
<configuration>
<property>
 <name>dfs.replication</name>
 <value>1</value>
</property>
EOF
# use /disk0, /disk1... as local storage
EXTRA_DISKS=`ls / | grep '^disk'`
DN=""
NN=""
for disk in $EXTRA_DISKS; do
 DN=file:///${disk}/dfs/dn,$DN
 NN=file:///${disk}/dfs/nn,$NN
done
if [[ "$DN" != "" ]]; then
 cat >> /hadoop/etc/hadoop/hdfs-site.xml << EOF
<property>
 <name>dfs.name.dir</name>
 <value>$NN</value>
</property>
<property>
 <name>dfs.data.dir</name>
 <value>$DN</value>
</property>
EOF
fi
cat >> /hadoop/etc/hadoop/hdfs-site.xml << EOF
</configuration>
EOF

cat > /hadoop/etc/hadoop/mapred-site.xml << EOF
<configuration>
<property>
 <name>mapred.job.tracker</name>
 <value>${namenode}:9001</value>
</property>
<property>
 <name>mapreduce.framework.name</name>
 <value>yarn</value>
</property>
</configuration>
EOF

cat > /hadoop/etc/hadoop/yarn-site.xml << EOF
<configuration>
<property>
<name>yarn.resourcemanager.resourcetracker.address</name>
<value>${namenode}:8025</value>
</property>
<property>
 <name>yarn.resourcemanager.scheduler.address</name>
 <value>${namenode}:8030</value>
</property>
<property>
 <name>yarn.resourcemanager.address</name>
 <value>${namenode}:8050</value>
</property>
<property>
 <name>yarn.resourcemanager.admin.address</name>
 <value>${namenode}:8041</value>
</property>
<property>
<name>yarn.resourcemanager.hostname</name>
<value>${namenode}</value>
</property>
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.vmem-check-enabled</name>
<value>false</value>
</property>
</configuration>
EOF
