#!/usr/bin/env bash

Usage="Usage: start-safe.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

while [ 1 ]
do
  RUN=`ps -ef | grep "tachyon.Master" | grep "java" | wc | cut -d" " -f7`

  if [[ $RUN -eq 0 ]] ; then
    echo "Restarting the system master..."
    $bin/start.sh
  fi
  echo "Tachyon is running... "
  sleep 2
done