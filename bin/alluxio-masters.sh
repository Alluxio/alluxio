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

USAGE="Usage: alluxio-masters.sh command..."

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  echo ${USAGE} >&2
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}/../libexec"
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

HOSTLIST=($(echo $(cat "${ALLUXIO_CONF_DIR}/masters" | sed  "s/#.*$//;/^$/d")))
mkdir -p "${ALLUXIO_LOGS_DIR}"
ALLUXIO_TASK_LOG="${ALLUXIO_LOGS_DIR}/task.log"

echo "Executing the following command on all master nodes and logging to ${ALLUXIO_TASK_LOG}: $@" | tee -a ${ALLUXIO_TASK_LOG}

N=0
HA_ENABLED=$(${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.zookeeper.enabled)
JOURNAL_TYPE=$(${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.master.journal.type | awk '{print toupper($0)}')
if [[ ${JOURNAL_TYPE} == "EMBEDDED" ]]; then
  HA_ENABLED="true"
fi
for master in ${HOSTLIST[@]}; do
  echo "[${master}] Connecting as ${USER}..." >> ${ALLUXIO_TASK_LOG}
  if [[ ${HA_ENABLED} == "true" || ${N} -eq 0 ]]; then
    nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${master} ${LAUNCHER} \
      $"${@// /\\ }" 2>&1 | while read line; do echo "[$(date '+%F %T')][${master}] ${line}"; done >> ${ALLUXIO_TASK_LOG} &
  else
    nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${master} ${LAUNCHER} \
      $"export ALLUXIO_MASTER_SECONDARY=true; ${@// /\\ }" 2>&1 | while read line; do echo "[$(date '+%F %T')][${master}] ${line}"; done >> ${ALLUXIO_TASK_LOG} &
  fi
  pids[${#pids[@]}]=$!
  N=$((N+1))
done

# wait for all pids
echo "Waiting for tasks to finish..."
has_error=0
for ((i=0; i< ${#pids[@]}; i++)); do
    wait ${pids[$i]}
    ret_code=$?
    if [[ ${ret_code} -ne 0 ]]; then
      has_error=1
      echo "Task on '${HOSTLIST[$i]}' fails, exit code: ${ret_code}" >&2
    fi
done

# only show the log when all tasks run OK!
if [[ ${has_error} -eq 0 ]]; then
    echo "All tasks finished"
else
    echo "There are task failures, look at ${ALLUXIO_TASK_LOG} for details." >&2
fi
