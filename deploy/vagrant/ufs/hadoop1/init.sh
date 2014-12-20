#!/bin/sh
set -e

HADOOP_VERSION="1.0.4"
NODES=`cat /tachyon/conf/workers`

cd /vagrant/shared

mkdir -p /tmp/hdfs-datanode
mkdir -p /tmp/hadoop-tmpstore

if [ ! -f hadoop-${HADOOP_VERSION}-bin.tar.gz ]
then
    # download hadoop
    echo "Downloading hadoop ${HADOOP_VERSION} ..." 
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}-bin.tar.gz  
    tar xzf hadoop-${HADOOP_VERSION}-bin.tar.gz  
fi

if [ ! -d /hadoop ]
then
    ln -s `pwd`/hadoop-${HADOOP_VERSION} /hadoop

    # setup hadoop
    rm -f /hadoop/conf/workers
    for i in ${NODES[@]}
    do 
        echo $i >> /hadoop/conf/workers
    done

    # choose the last node as namenode
    namenode=$i
    mkdir -p /hadoop/tmp-store
    cat > /hadoop/conf/core-site.xml << EOF
<configuration>
   <property>
      <name>hadoop.tmp.dir</name>
      <value>/tmp/hadoop-tmpstore</value>
   </property>
   <property>
      <name>fs.default.name</name>
      <value>hdfs://${namenode}:9000</value>
   </property>
</configuration>
EOF

    cat > /hadoop/conf/hdfs-site.xml << EOF
<configuration>
  <property>
     <name>dfs.replication</name>
     <value>1</value>
  </property>
  <property>
     <name>dfs.data.dir</name>
     <value>/tmp/hdfs-datanode</value>
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

    # create tachyon env
    /bin/cp /tachyon/conf/tachyon-env.sh.template /tachyon/conf/tachyon-env.sh 
    sed -i "s/#export TACHYON_UNDERFS_ADDRESS=hdfs:\/\/localhost:9000/export TACHYON_UNDERFS_ADDRESS=hdfs:\/\/${namenode}:9000/g" /tachyon/conf/tachyon-env.sh
    sed -i "s/export TACHYON_MASTER_ADDRESS=localhost/export TACHYON_MASTER_ADDRESS=${namenode}/g" /tachyon/conf/tachyon-env.sh

fi

# setup tachyon
cd /tachyon
mvn -q clean package install -DskipTests -Dhadoop.version=${HADOOP_VERSION}
