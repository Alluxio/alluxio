#!/bin/sh

HADOOP_VERSION="2.4.1"
nodes=`cat /tachyon/conf/slaves`

cd /vagrant/hadoop2

if [ ! -f hadoop-${HADOOP_VERSION}.tar.gz ]
then
    # download hadoop
    echo "Downloading hadoop ..."
    wget -q http://www.us.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz  
    tar xzf hadoop-${HADOOP_VERSION}.tar.gz  
fi

if [ ! -f /hadoop ]
then
    ln -fs `pwd`/hadoop-${HADOOP_VERSION} /hadoop

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
