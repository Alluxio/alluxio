#!/bin/sh

# start hadoop
/hadoop/bin/hdfs namenode -format
/hadoop/sbin/start-all.sh
echo "check hadoop processes ..."
jps
echo "check node list..."
until /hadoop/bin/yarn node -list all; do
  sleep 1;
done
echo "check storage space..."
until /hadoop/bin/hdfs dfsadmin -report; do
  sleep 1;
done;
