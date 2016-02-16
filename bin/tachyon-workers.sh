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

HOSTLIST=$(cat ${TACHYON_CONF_DIR}/workers | sed  "s/#.*$//;/^$/d")
TACHYON_TASK_LOG="$(echo ${BIN} | sed 's/bin$//g')"logs/task.log

for worker in $(echo ${HOSTLIST}); do
  echo "$(date +"%F %H:%M:%S,$(date +"%s%N" | cut -c 11- | cut -c 1-3)") INFO ${WORKER_ACTION_TYPE}  Connecting to $worker as $USER..." >> ${TACHYON_TASK_LOG}
  nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t ${worker} $LAUNCHER $"${@// /\\ }" >> ${TACHYON_TASK_LOG} 2>&1&
done

wait
