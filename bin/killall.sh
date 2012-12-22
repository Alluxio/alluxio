#!/usr/bin/env bash

if [ "$#" -lt 1 ]
then 
  echo "Usage: $0 <key word>"
  exit
fi

keyword=$1
count=0
for pid in `ps -A -o pid,command | grep -i "[j]ava" | grep $keyword | awk '{print $1}'`; do
  kill -9 $pid 2> /dev/null
  count=`expr $count + 1`
done
echo "Killed $count processes"
