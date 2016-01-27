#!/usr/bin/env bash

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

usage="Usage: tachyon-workers.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $TACHYON_LIBEXEC_DIR/tachyon-config.sh

HOSTLIST=$TACHYON_CONF_DIR/workers

for worker in `cat "$HOSTLIST" | sed  "s/#.*$//;/^$/d"`; do
  echo "Connecting to $worker as $USER..."
  if [ -n "${TACHYON_SSH_FOREGROUND}" ]; then
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t $worker $LAUNCHER $"${@// /\\ }" 2>&1
  else
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t $worker $LAUNCHER $"${@// /\\ }" 2>&1 &
  fi
  if [ "$TACHYON_WORKER_SLEEP" != "" ]; then
    sleep $TACHYON_WORKER_SLEEP
  fi
done

wait
