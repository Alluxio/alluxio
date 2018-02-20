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
BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: alluxio-monitor.sh ACTION
Where ACTION is one of:
  all                \tStart monitors for all masters, proxies, and workers nodes.
  local              \tStart monitors for all process locally.
  master             \tStart a master monitor on this node.
  masters            \tStart monitors for all masters nodes.
  worker             \tStart a worker monitor on this node.
  workers            \tStart monitors for all workers nodes.
  proxy              \tStart the proxy monitor on this node.
  proxies            \tStart monitors for all proxies nodes."

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

prepare_monitor() {
  local msg=$1
  echo "*****************************************"
  echo "${msg}"
  echo "*****************************************"
  sleep 2
}

run_monitor() {
  local node_type=$1
  local node_logs=( "${ALLUXIO_LOGS_DIR}/${node_type}.log" "${ALLUXIO_LOGS_DIR}/${node_type}.out" )
  case "${node_type}" in
    master)
      monitor_exec=alluxio.master.AlluxioMasterMonitor
      ;;
    worker)
      monitor_exec=alluxio.worker.AlluxioWorkerMonitor
      ;;
    proxy)
      monitor_exec=alluxio.proxy.AlluxioProxyMonitor
      ;;
    *)
      echo "Error: Invalid NODE_TYPE: ${node_type}" >&2
      exit 1
      ;;
  esac

  echo " - Running ${node_type} monitor @ $(hostname -f)."
  "${JAVA}" -cp ${CLASSPATH} ${ALLUXIO_JAVA_OPTS} ${monitor_exec}
  if [[ $? -ne 0 ]]; then
    echo -e " - ${RED}[ FAILED ] The ${node_type} @ $(hostname -f) is not serving requests.${NC}"
    for node_log in "${node_logs[@]}"; do
      echo " - Printing the log tail for ${node_log}"
      echo ">> BEGIN"
      test -f "${node_log}" && tail -30 "${node_log}"
      echo "<< EOF"
    done
  else
    echo -e "   ${GREEN}[ OK ] The ${node_type} service @ $(hostname -f) is in an healthy state.${NC}"
  fi
}

run_monitors() {
  local node_type=$1
  local workers=$(cat "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d")
  local masters=$(cat "${ALLUXIO_CONF_DIR}/masters" | sed  "s/#.*$//;/^$/d")
  local proxies=$(cat "${ALLUXIO_CONF_DIR}/masters" "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d" | sort | uniq)
  local nodes=
  case "${node_type}" in
    master)
      nodes=${masters}
      ;;
    worker)
      nodes=${workers}
      ;;
    proxy)
      nodes=${proxies}
      ;;
    *)
      echo "Error: Invalid NODE_TYPE: ${node_type}" >&2
      exit 1
      ;;
  esac

  for node in $(echo ${nodes}); do
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${node} ${LAUNCHER} \
        "${BIN}/alluxio-monitor.sh" "${node_type}"
  done
}

main() {
  
  # if no args specified, show usage
  if [[ $# -ne 1 ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi

  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
  CLASSPATH=${ALLUXIO_SERVER_CLASSPATH}

  local ACTION=$1
  case "${ACTION}" in
    all)
      prepare_monitor "Starting to monitor all the services."
      run_monitors "master"
      run_monitors "worker"
      run_monitors "proxy"
      ;;
    local)
      prepare_monitor "Starting to monitor the local services."
      run_monitor "master"
      run_monitor "worker"
      run_monitor "proxy"
      ;;
    master)
      prepare_monitor "Starting to monitor the master service."
      run_monitor "master"
      ;;
    masters)
      prepare_monitor "Starting to monitor all the masters services."
      run_monitors "master"
      ;;
    proxy)
      prepare_monitor "Starting to monitor the proxy service."
      run_monitor "proxy"
      ;;
    proxies)
      prepare_monitor "Starting to monitor the proxies services."
      run_monitors "proxy"
      ;;
    worker)
      prepare_monitor "Starting to monitor the worker service."
      run_monitor "worker"
      ;;
    workers)
      prepare_monitor "Starting to monitor the workers services."
      run_monitors "worker"
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}" >&2
    echo -e "${USAGE}" >&2
    exit 1
  esac
}

main "$@"
