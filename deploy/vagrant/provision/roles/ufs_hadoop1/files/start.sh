#!/bin/sh

# start hadoop
/hadoop/bin/hadoop namenode -format
/hadoop/bin/start-dfs.sh
/hadoop/bin/start-mapred.sh
echo "check hadoop processes ..."
jps
echo "wait safe mode off ..."
/hadoop/bin/hadoop dfsadmin -safemode wait
echo "check storage space..."
until /hadoop/bin/hadoop dfsadmin -report; do
  sleep 1;
done;
