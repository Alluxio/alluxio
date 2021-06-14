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
  job_master         \tStart a job_master monitor on this node.
  job_masters        \tStart monitors for all job_master nodes.
  job_worker         \tStart a job_worker monitor on this node.
  job_workers        \tStart monitors for all job_worker nodes.
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
WHITE='\033[1;37m'
NC='\033[0m'

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
  CLASSPATH=${ALLUXIO_CLIENT_CLASSPATH}
  ALLUXIO_TASK_LOG="${ALLUXIO_LOGS_DIR}/task.log"

  # Remove the remote debug configuration to avoid the error: "transport error 20: bind failed: Address already in use." 
  # See https://github.com/Alluxio/alluxio/issues/10958
  ALLUXIO_MASTER_MONITOR_JAVA_OPTS=$(echo ${ALLUXIO_MASTER_JAVA_OPTS} | sed 's/^-agentlib:jdwp=transport=dt_socket.*address=[0-9]*//')
  ALLUXIO_WORKER_MONITOR_JAVA_OPTS=$(echo ${ALLUXIO_WORKER_JAVA_OPTS} | sed 's/^-agentlib:jdwp=transport=dt_socket.*address=[0-9]*//')
  ALLUXIO_JOB_MASTER_MONITOR_JAVA_OPTS=$(echo ${ALLUXIO_JOB_MASTER_JAVA_OPTS} | sed 's/^-agentlib:jdwp=transport=dt_socket.*address=[0-9]*//')
  ALLUXIO_JOB_WORKER_MONITOR_JAVA_OPTS=$(echo ${ALLUXIO_JOB_WORKER_JAVA_OPTS} | sed 's/^-agentlib:jdwp=transport=dt_socket.*address=[0-9]*//')
}

prepare_monitor() {
  local msg=$1
  echo -e "${WHITE}-----------------------------------------${NC}"
  echo -e "${msg}"
  echo -e "${WHITE}-----------------------------------------${NC}"
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
  local alluxio_config="${ALLUXIO_JAVA_OPTS}"

  case "${node_type}" in
    master)
      monitor_exec=alluxio.master.AlluxioMasterMonitor
      alluxio_config="${alluxio_config} ${ALLUXIO_MASTER_MONITOR_JAVA_OPTS}"
      ;;
    worker)
      monitor_exec=alluxio.worker.AlluxioWorkerMonitor
      alluxio_config="${alluxio_config} ${ALLUXIO_WORKER_MONITOR_JAVA_OPTS}"
      ;;
    job_master)
      monitor_exec=alluxio.master.job.AlluxioJobMasterMonitor
      alluxio_config="${alluxio_config} ${ALLUXIO_JOB_MASTER_MONITOR_JAVA_OPTS}"
      ;;
    job_worker)
      monitor_exec=alluxio.worker.job.AlluxioJobWorkerMonitor
      alluxio_config="${alluxio_config} ${ALLUXIO_JOB_WORKER_MONITOR_JAVA_OPTS}"
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
    "${JAVA}" -cp ${CLASSPATH} ${alluxio_config} ${monitor_exec}
    if [[ $? -ne 0 ]]; then
      echo -e "${WHITE}---${NC} ${RED}[ FAILED ]${NC} The ${CYAN}${node_type}${NC} @ ${PURPLE}$(hostname -f)${NC} is not serving requests.${NC}"
      print_node_logs "${node_type}"
      return 1
    fi
  fi
  echo -e "${WHITE}---${NC} ${GREEN}[ OK ]${NC} The ${CYAN}${node_type}${NC} service @ ${PURPLE}$(hostname -f)${NC} is in a healthy state.${NC}"
  return 0
}

# $1 --> The path to the masters/workers file
get_nodes() {
    local node_file=$1
    echo "$(cat "${node_file}" | sed  "s/#.*$//;/^$/d")"
}

run_on_node() {
  local node=$1
  # Appends every argument after $1 to the launcher terminal

  ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${node} ${LAUNCHER} ${@:2} \
    2> >(while read line; do echo "[$(date '+%F %T')][${node}] ${line}" >> ${ALLUXIO_TASK_LOG}; done)
}

run_monitors() {
  local node_type=$1
  local nodes=$2
  local mode=$3
  if [[ -z "${nodes}" ]]; then
    case "${node_type}" in
      master)
        nodes=$(get_nodes "${ALLUXIO_CONF_DIR}/masters")
        ;;
      worker)
        nodes=$(get_nodes "${ALLUXIO_CONF_DIR}/workers")
        ;;
      job_master)
        # Fall back to {conf}/masters if job_masters doesn't exist
        local job_masters="${ALLUXIO_CONF_DIR}/job_masters"
        if [[ ! -f ${job_masters} ]]; then job_masters=${ALLUXIO_CONF_DIR}/masters; fi
        nodes=$(get_nodes "${job_masters}")
        ;;
      job_worker)
        # Fall back to {conf}/workers if job_workers doesn't exist
        local job_workers="${ALLUXIO_CONF_DIR}/job_workers"
        if [[ ! -f ${job_workers} ]]; then job_workers=${ALLUXIO_CONF_DIR}/workers; fi
        nodes=$(get_nodes "${job_workers}")
        ;;
      proxy)
        nodes=$(awk '{print}' "${ALLUXIO_CONF_DIR}/masters" "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d" | sort | uniq)
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
    run_on_node ${master} "${BIN}/alluxio-monitor.sh" ${mode} "${node_type}"

    nodes=$(echo -e "${nodes}" | tail -n+2)
    if [[ $? -ne 0 ]]; then
      # if there is an error, print the log tail for the remaining master nodes.
      batch_run_on_nodes "$(echo ${nodes})" "${BIN}/alluxio-monitor.sh" -L "${node_type}"
    else
      HA_ENABLED=$(${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.zookeeper.enabled)
      JOURNAL_TYPE=$(${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} alluxio.master.journal.type | awk '{print toupper($0)}')
      if [[ ${JOURNAL_TYPE} == "EMBEDDED" ]]; then
        HA_ENABLED="true"
      fi
      if [[ ${HA_ENABLED} == "true" ]]; then
        batch_run_on_nodes "$(echo ${nodes})" "${BIN}/alluxio-monitor.sh" "${mode}" "${node_type}"
      fi
    fi
  else
    batch_run_on_nodes "$(echo ${nodes})" "${BIN}/alluxio-monitor.sh" "${mode}" "${node_type}"
  fi
}

# Used to run a command on multiple hosts concurrently.
# By default it limits concurrent tasks to 100.
batch_run_on_nodes() {
  # String of nodes, seperated by a new line
  local nodes=$1
  # Command to run on each node
  local command=$2
  # Parameter for command
  local params=${@:3}

  # How many nodes to run on concurrently
  local batchCount=100

  local taskCount=0
  for node in $nodes; do
      run_on_node ${node} ${command} ${params} &

      # Wait for existing tasks, if batch is full
      ((taskCount++))
      if [ $(( $taskCount % $batchCount )) == 0 ]; then wait; fi;
  done
  wait
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
      run_monitors "master"     "" "${MODE}"
      run_monitors "job_master" "" "${MODE}"
      run_monitors "worker"     "" "${MODE}"
      run_monitors "job_worker" "" "${MODE}"
      run_monitors "proxy"      "" "${MODE}"
      ;;
    local)
      prepare_monitor "Starting to monitor ${CYAN}all local${NC} services."
      run_monitor "master"      "${MODE}"
      run_monitor "job_master"  "${MODE}"
      run_monitor "worker"      "${MODE}"
      run_monitor "job_worker"  "${MODE}"
      run_monitor "proxy"       "${MODE}"
      ;;
    master)
      run_monitor "master" "${MODE}"
      ;;
    masters)
      prepare_monitor "Starting to monitor all Alluxio ${CYAN}masters${NC}."
      run_monitors "master" "${HOSTS}"
      ;;
    job_master)
      run_monitor "job_master" "${MODE}"
      ;;
    job_masters)
      prepare_monitor "Starting to monitor all Alluxio ${CYAN}job masters${NC}."
      run_monitors "job_master" "${HOSTS}"
      ;;
    job_worker)
      run_monitor "job_worker" "${MODE}"
      ;;
    job_workers)
      prepare_monitor "Starting to monitor all Alluxio ${CYAN}job workers${NC}."
      run_monitors "job_worker" "${HOSTS}"
      ;;
    proxy)
      run_monitor "proxy" "${MODE}"
      ;;
    proxies)
      prepare_monitor "Starting to monitor all Alluxio ${CYAN}proxies${NC}."
      run_monitors "proxy" "${HOSTS}" "${MODE}"
      ;;
    worker)
      run_monitor "worker" "${MODE}"
      ;;
    workers)
      prepare_monitor "Starting to monitor all Alluxio ${CYAN}workers${NC}."
      run_monitors "worker" "${HOSTS}" "${MODE}"
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}" >&2
    echo -e "${USAGE}" >&2
    exit 1
  esac
}

main "$@"
