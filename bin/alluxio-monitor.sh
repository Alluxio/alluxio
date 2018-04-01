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

USAGE="Usage: alluxio-monitor.sh [-hL] ACTION [host1,host2,...]
Where ACTION is one of:
  all                \tStart monitors for all masters, proxies, and workers nodes.
  local              \tStart monitors for all process locally.
  master             \tStart a master monitor on this node.
  masters            \tStart monitors for all masters nodes.
  worker             \tStart a worker monitor on this node.
  workers            \tStart monitors for all workers nodes.
  proxy              \tStart the proxy monitor on this node.
  proxies            \tStart monitors for all proxies nodes.

[host1,host2,...] is a comma separated list of host to monitor, if not given the default config for the target is used.

-L  enables the log mode, this option disable the monitor checks and causes alluxio-monitor to only print the node log tail.
-h  display this help.
"

RED='\033[1;31m'
GREEN='\033[1;32m'
PURPLE='\033[0;35m'
CYAN='\033[1;36m'
NC='\033[0m'
BOLD=$(tput bold)
END_BOLD=$(tput sgr0)

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
  CLASSPATH=${ALLUXIO_CLIENT_CLASSPATH}
}

prepare_monitor() {
  local msg=$1
  echo -e "${BOLD}-----------------------------------------${END_BOLD}"
  echo -e "${msg}"
  echo -e "${BOLD}-----------------------------------------${END_BOLD}"
  sleep 2
}

print_node_logs() {
  local node_type=$1
  local node_logs=( "${ALLUXIO_LOGS_DIR}/${node_type}.log" "${ALLUXIO_LOGS_DIR}/${node_type}.out" )
  for node_log in "${node_logs[@]}"; do
    echo -e "${CYAN}--- Printing the log tail for ${node_log}${NC}"
    if [[ -f "${node_log}" ]]; then
      local lines_count=$(cat "${node_log}" | wc -l)
      if [[ lines_count -gt 0 ]]; then
        echo -e "${CYAN}>>> BEGIN${NC}"
        tail -30 "${node_log}"
        echo -e "${CYAN}<<< EOF${NC}"
      else
        echo -e "    --- EMPTY ---"
      fi
    fi
  done
}

run_monitor() {
  local node_type=$1
  local mode=$2
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
      return 1
      ;;
  esac

  if [[ "${mode}" == "-L" ]]; then
    print_node_logs "${node_type}"
    return 0
  else
    echo -e "${BOLD}---${END_BOLD} Running ${CYAN}${node_type}${NC} monitor ${BOLD}@ $(hostname -f)${END_BOLD}."
    "${JAVA}" -cp ${CLASSPATH} ${ALLUXIO_JAVA_OPTS} ${monitor_exec}
    if [[ $? -ne 0 ]]; then
      echo -e "${BOLD}---${END_BOLD} ${RED}[ FAILED ] The ${node_type} @ $(hostname -f) is not serving requests.${NC}"
      print_node_logs "${node_type}"
      return 1
    fi
  fi
  echo -e "${BOLD}---${END_BOLD} ${GREEN}[ OK ] The ${node_type} service @ $(hostname -f) is in an healthy state.${NC}"
  return 0
}

run_monitors() {
  local node_type=$1
  local nodes=$2
  local mode=$3
  if [[ -z "${nodes}" ]]; then
    case "${node_type}" in
      master)
        nodes=$(cat "${ALLUXIO_CONF_DIR}/masters" | sed  "s/#.*$//;/^$/d")
        ;;
      worker)
        nodes=$(cat "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d")
        ;;
      proxy)
        nodes=$(cat "${ALLUXIO_CONF_DIR}/masters" "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d" | sort | uniq)
        ;;
      *)
        echo "Error: Invalid NODE_TYPE: ${node_type}" >&2
        exit 1
        ;;
    esac
  fi

  if [[ "${node_type}" == "master" ]]; then
    # master check should only run once...
    local master=$(echo -e "${nodes}" | head -n1)
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${master} ${LAUNCHER} \
        "${BIN}/alluxio-monitor.sh" ${mode} "${node_type}"
    if [[ $? -ne 0 ]]; then
      # if there is an error, print the log tail for the remaining master nodes.
      nodes=$(echo -e "${nodes}" | tail -n+2)
      for node in $(echo ${nodes}); do
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${node} ${LAUNCHER} \
            "${BIN}/alluxio-monitor.sh" -L "${node_type}"
      done
    fi
  else
    for node in $(echo ${nodes}); do
      ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${node} ${LAUNCHER} \
          "${BIN}/alluxio-monitor.sh" ${mode} "${node_type}"
    done
  fi
}

main() {

  # exceed the max number of args, show usage
  if [[ $# -ge 3 ]]; then
    echo "Error: Invalid number of arguments" >&2
    echo -e "${USAGE}" >&2
    exit 1
  fi

  get_env

  while getopts "hL" o; do
    case "${o}" in
      h)
        echo -e "${USAGE}"
        exit 0
        ;;
      L)
        MODE="-L"
        ;;
      *)
        echo -e "${USAGE}" >&2
        exit 1
        ;;
    esac
  done

  shift $((${OPTIND} - 1))

  ACTION=$1
  shift

  HOSTS=$1
  if [[ ! -z "${HOSTS}" ]]; then
    HOSTS=$(echo "${HOSTS}" | tr ',' '\n')
  fi

  case "${ACTION}" in
    all)
      prepare_monitor "Starting to monitor ${CYAN}all remote${NC} services."
      run_monitors "master" "${HOSTS}" "${MODE}"
      run_monitors "worker" "${HOSTS}" "${MODE}"
      run_monitors "proxy" "${HOSTS}" "${MODE}"
      ;;
    local)
      prepare_monitor "Starting to monitor ${CYAN}all local${NC} services."
      run_monitor "master" "${MODE}"
      run_monitor "worker" "${MODE}"
      run_monitor "proxy"  "${MODE}"
      ;;
    master)
      prepare_monitor "Starting to monitor the ${CYAN}master${NC} service."
      run_monitor "master" "${MODE}"
      ;;
    masters)
      prepare_monitor "Starting to monitor all the ${CYAN}masters${NC} services."
      run_monitors "master" "${HOSTS}"
      ;;
    proxy)
      prepare_monitor "Starting to monitor the ${CYAN}proxy${NC} service."
      run_monitor "proxy" "${MODE}"
      ;;
    proxies)
      prepare_monitor "Starting to monitor the ${CYAN}proxies${NC} services."
      run_monitors "proxy" "${HOSTS}" "${MODE}"
      ;;
    worker)
      prepare_monitor "Starting to monitor the ${CYAN}worker${NC} service."
      run_monitor "worker" "${MODE}"
      ;;
    workers)
      prepare_monitor "Starting to monitor the ${CYAN}workers${NC} services."
      run_monitors "worker" "${HOSTS}" "${MODE}"
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}" >&2
    echo -e "${USAGE}" >&2
    exit 1
  esac
}

main "$@"
