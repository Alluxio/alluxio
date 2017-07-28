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

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

USAGE="Usage: alluxio-masters.sh command..."

# if no args specified, show usage
if [[ $# -le 0 ]]; then
  echo ${USAGE} >&2
  exit 1
fi

DEFAULT_LIBEXEC_DIR="${BIN}/../libexec"
ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
. ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

HOSTLIST=$(cat "${ALLUXIO_CONF_DIR}/masters" | sed  "s/#.*$//;/^$/d")
mkdir -p "${ALLUXIO_LOGS_DIR}"
ALLUXIO_TASK_LOG="${ALLUXIO_LOGS_DIR}/task.log"

echo "Executing the following command on all master nodes and logging to ${ALLUXIO_TASK_LOG}: $@" | tee -a ${ALLUXIO_TASK_LOG}

N=0
ZOOKEEPER_ENABLED=$(${BIN}/alluxio getConf alluxio.zookeeper.enabled)
for master in $(echo ${HOSTLIST}); do
  echo "[${master}] Connecting as ${USER}..." >> ${ALLUXIO_TASK_LOG}
  if [[ ${ZOOKEEPER_ENABLED} == "true" || ${N} -eq 0 ]]; then
    nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${master} ${LAUNCHER} \
      $"${@// /\\ }" 2>&1 | while read line; do echo "[${master}] ${line}"; done >> ${ALLUXIO_TASK_LOG} &
  else
    nohup ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${master} ${LAUNCHER} \
      $"export ALLUXIO_MASTER_SECONDARY=true; ${@// /\\ }" 2>&1 | while read line; do echo "[${master}] ${line}"; done >> ${ALLUXIO_TASK_LOG} &
  fi
  N=$((N+1))
done

echo "Waiting for tasks to finish..."
wait
echo "All tasks finished"
