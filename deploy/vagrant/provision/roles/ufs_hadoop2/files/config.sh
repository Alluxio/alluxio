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
