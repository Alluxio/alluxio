#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

set -o pipefail

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: alluxio-workers.sh command..."

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  echo ${USAGE} >&2
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}/../libexec"
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

HOSTLIST=($(echo $(cat "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d")))
mkdir -p "${ALLUXIO_LOGS_DIR}"
ALLUXIO_TASK_LOG="${ALLUXIO_LOGS_DIR}/task.log"

echo "Executing the following command on all worker nodes and logging to ${ALLUXIO_TASK_LOG}: $@" | tee -a ${ALLUXIO_TASK_LOG}

for worker in ${HOSTLIST[@]}; do
  echo "[${worker}] Connecting as ${USER}..." >> ${ALLUXIO_TASK_LOG}
  nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${worker} ${LAUNCHER} \
    $"${@// /\\ }" 2>&1 | while read line; do echo "[$(date '+%F %T')][${worker}] ${line}"; done >> ${ALLUXIO_TASK_LOG} &
  pids[${#pids[@]}]=$!
done

# wait for all pids
echo "Waiting for tasks to finish..."
has_error=0
for ((i=0; i< ${#pids[@]}; i++)); do
    wait ${pids[$i]}
    ret_code=$?
    if [[ ${ret_code} -ne 0 ]]; then
      has_error=1
      echo "Task on '${HOSTLIST[$i]}' fails, exit code: ${ret_code}"
    fi
done

# only show the log when all tasks run OK!
if [[ ${has_error} -eq 0 ]]; then
    echo "All tasks finished"
else
    echo "There are task failures, look at ${ALLUXIO_TASK_LOG} for details."
fi
