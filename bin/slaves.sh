#!/usr/bin/env bash

usage="Usage: slaves.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

HOSTLIST=$bin/../conf/slaves

for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no $slave $"${@// /\\ }" 2>&1 | sed "s/^/$slave: /" &
  sleep 0.001
done

wait
