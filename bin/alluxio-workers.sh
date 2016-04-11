#!/usr/bin/env bash

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

USAGE="Usage: alluxio-workers.sh command..."

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  echo ${USAGE}
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

HOSTLIST=$(cat ${ALLUXIO_CONF_DIR}/workers | sed  "s/#.*$//;/^$/d")
ALLUXIO_TASK_LOG="$(echo ${BIN} | sed 's/bin$//g')"logs/task.log

if [[ "$3" == "alluxio.worker.AlluxioWorker" ]]; then
  WORKER_ACTION_TYPE="WORKERS"
else
  WORKER_ACTION_TYPE="MASTER"
fi

for worker in $(echo ${HOSTLIST}); do
  echo "$(date +"%F %H:%M:%S,$(date +"%s%N" | cut -c 11- | cut -c 1-3)")
   INFO ${WORKER_ACTION_TYPE}  Connecting to ${worker} as ${USER}..." >> ${ALLUXIO_TASK_LOG}
  nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -t ${worker} ${LAUNCHER} \
   $"${@// /\\ }" >> ${ALLUXIO_TASK_LOG} 2>&1&
done

echo "Waiting for ${WORKER_ACTION_TYPE} tasks to finish..."
wait
echo "All ${WORKER_ACTION_TYPE} tasks finished, please analyze the log at ${ALLUXIO_TASK_LOG}."
