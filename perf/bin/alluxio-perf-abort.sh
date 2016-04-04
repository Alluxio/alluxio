#!/usr/bin/env bash

function printUsage {
  echo "Usage: alluxio-perf-abort.sh"
}

# if more than 0 args specified, show usage
if [ $# -ge 1 ]; then
  printUsage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

count=0;
for pid in `ps -A -o pid,command | grep -i "[j]ava" | grep "alluxio.perf.AlluxioPerfSlave" | awk '{print $1}'`; do
  kill -9 $pid 2> /dev/null
  count=`expr $count + 1`
done
echo "Killed $count processes"

sleep 1
