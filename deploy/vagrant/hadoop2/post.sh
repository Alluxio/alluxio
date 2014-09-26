#!/bin/sh

# start hadoop
/hadoop/bin/hdfs namenode -format
/hadoop/sbin/start-all.sh
echo "check hadoop processes ..."
jps
echo "check node list..."
/hadoop/bin/yarn node -list
