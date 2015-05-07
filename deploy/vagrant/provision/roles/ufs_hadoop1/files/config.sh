NODES=`cat /vagrant/files/workers`

# setup hadoop
rm -f /hadoop/conf/slaves
for i in ${NODES[@]}
do 
    echo $i >> /hadoop/conf/slaves
done

# choose the last node as namenode
namenode=$i
echo $namenode > /hadoop/conf/masters

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
