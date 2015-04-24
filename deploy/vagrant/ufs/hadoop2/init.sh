#!/bin/sh
set -e

HADOOP_VERSION="2.4.1"
nodes=`cat /tachyon/conf/workers`

cd /vagrant/shared

if [ ! -f hadoop-${HADOOP_VERSION}.tar.gz ]
then
    # download hadoop
    echo "Downloading hadoop ${HADOOP_VERSION} ..." 
    wget -q http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz
fi

if [ ! -d /hadoop ]
then
    sudo mkdir /hadoop && sudo chown -R `whoami` /hadoop
    cp -R `pwd`/hadoop-${HADOOP_VERSION}/* /hadoop

    # setup hadoop
    rm -f /hadoop/etc/hadoop/slaves
    for i in ${nodes[@]}
    do 
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
</configuration>
EOF

    cat > /hadoop/etc/hadoop/hdfs-site.xml << EOF
<configuration>
  <property>
     <name>dfs.replication</name>
     <value>1</value>
  </property>
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
</configuration>
EOF
fi
