#!/usr/bin/env bash

usage="Usage: tachyon-workers.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`
DEFAULT_LIBEXEC_DIR="$bin"/../libexec
TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $TACHYON_LIBEXEC_DIR/tachyon-config.sh

HOSTLIST=$TACHYON_CONF_DIR/workers

for worker in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
  echo -n "Connection to $worker... "
  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t $worker $"${@// /\\ }" 2>&1
  sleep 0.02
done

wait
