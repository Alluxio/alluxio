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

#start up alluxio

USAGE="Usage: alluxio-start.sh [-hNwm] [-i backup] ACTION [MOPT] [-f]
Where ACTION is one of:
  all [MOPT]         \tStart all masters, proxies, and workers.
  job_master         \tStart the job_master on this node.
  job_masters        \tStart job_masters on master nodes.
  job_worker         \tStart a job_worker on this node.
  job_workers        \tStart job_workers on worker nodes.
  local [MOPT]       \tStart all processes locally.
  master             \tStart the local master on this node.
  secondary_master   \tStart the local secondary master on this node.
  masters            \tStart masters on master nodes.
  proxy              \tStart the proxy on this node.
  proxies            \tStart proxies on master and worker nodes.
  safe               \tScript will run continuously and start the master if it's not running.
  worker [MOPT]      \tStart a worker on this node.
  workers [MOPT]     \tStart workers on worker nodes.
  logserver          \tStart the logserver
  restart_worker     \tRestart a failed worker on this node.
  restart_workers    \tRestart any failed workers on worker nodes.

MOPT (Mount Option) is one of:
  Mount    \tMount the configured RamFS if it is not already mounted.
  SudoMount\tMount the configured RamFS using sudo if it is not already mounted.
  NoMount  \tDo not mount the configured RamFS.
           \tNotice: to avoid sudo requirement but using tmpFS in Linux,
             set ALLUXIO_RAM_FOLDER=/dev/shm on each worker and use NoMount.
  NoMount is assumed if MOPT is not specified.

-a         asynchonously start all processes. The script may exit before all
           processes have been started.
-f         format Journal, UnderFS Data and Workers Folder on master.
-h         display this help.
-i backup  a journal backup to restore the master from. The backup should be
           a URI path within the root under filesystem, e.g.
           hdfs://mycluster/alluxio_backups/alluxio-journal-YYYY-MM-DD-timestamp.gz.
-N         do not try to kill previous running processes before starting new ones.
-w         wait for processes to end before returning.

Supported environment variables:

ALLUXIO_JOB_WORKER_COUNT - identifies how many job workers to start per node (default = 1)"

ensure_dirs() {
  if [[ ! -d "${ALLUXIO_LOGS_DIR}" ]]; then
    echo "ALLUXIO_LOGS_DIR: ${ALLUXIO_LOGS_DIR}"
    mkdir -p ${ALLUXIO_LOGS_DIR}
  fi
}

# returns 1 if "$1" contains "$2", 0 otherwise.
contains() {
  if [[ "$1" = *"$2"* ]]; then
    return 1
  fi
  return 0
}

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
  CLASSPATH=${ALLUXIO_SERVER_CLASSPATH}
}

# Pass ram folder to check as $1
# Return 0 if ram folder is mounted as tmpfs or ramfs, 1 otherwise
is_ram_folder_mounted() {
  local mounted_fs=""
  if [[ $(uname -s) == Darwin ]]; then
    mounted_fs=$(mount -t "hfs" | grep '/Volumes/' | cut -d " " -f 3)
  else
    mounted_fs=$(mount -t "tmpfs,ramfs" | cut -d " " -f 3)
  fi

  for fs in ${mounted_fs}; do
    if [[ "${1}" == "${fs}" || "${1}" =~ ^"${fs}"\/.* ]]; then
      return 0
    fi
  done

  return 1
}

check_mount_mode() {
  case $1 in
    Mount);;
    SudoMount);;
    NoMount)
      local tier_alias=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
      local tier_path=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)
      if [[ ${tier_alias} != "MEM" ]]; then
        # if the top tier is not MEM, skip check
        return
      fi
      is_ram_folder_mounted "${tier_path}"
      if [[ $? -ne 0 ]]; then
        echo "ERROR: Ramdisk ${tier_path} is not mounted with mount option NoMount. Use alluxio-mount.sh to mount ramdisk." >&2
        echo -e "${USAGE}" >&2
        exit 1
      fi
      if [[ "${tier_path}" =~ ^"/dev/shm"\/{0,1}$ ]]; then
        echo "WARNING: Using tmpFS does not guarantee data to be stored in memory."
        echo "WARNING: Check vmstat for memory statistics (e.g. swapping)."
      fi
      ;;
    *)
      if [[ -z $1 ]]; then
        echo "This command requires a mount mode be specified" >&2
      else
        echo "Invalid mount mode: $1" >&2
      fi
      echo -e "${USAGE}" >&2
      exit 1
  esac
}

# pass mode as $1
do_mount() {
  MOUNT_FAILED=0
  case "$1" in
    Mount|SudoMount)
      local tier_alias=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
      local tier_path=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)

      if [[ ${tier_alias} != "MEM" ]]; then
        echo "Can't Mount/SudoMount when alluxio.worker.tieredstore.level0.alias is not MEM"
        exit 1
      fi

      is_ram_folder_mounted "${tier_path}" # Returns 0 if already mounted.
      if [[ $? -eq 0 ]]; then
        echo "Ramdisk already mounted. Skipping mounting procedure."
      else
        echo "Ramdisk not detected. Mounting..."
        ${LAUNCHER} "${BIN}/alluxio-mount.sh" $1
        MOUNT_FAILED=$?
      fi
      ;;
    NoMount)
      ;;
    *)
      echo "This command requires a mount mode be specified" >&2
      echo -e "${USAGE}" >&2
      exit 1
  esac
}

stop() {
  ${BIN}/alluxio-stop.sh $1
}

start_job_master() {
  if [[ "$1" == "-f" ]]; then
    ${LAUNCHER} "${BIN}/alluxio" format
  fi

  if [[ ${ALLUXIO_MASTER_SECONDARY} != "true" ]]; then
    if [[ -z ${ALLUXIO_JOB_MASTER_JAVA_OPTS} ]] ; then
      ALLUXIO_JOB_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
    fi

    echo "Starting job master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} \
     ${ALLUXIO_JOB_MASTER_JAVA_OPTS} \
     alluxio.master.AlluxioJobMaster > ${ALLUXIO_LOGS_DIR}/job_master.out 2>&1) &
   fi
}

start_job_masters() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-start.sh" "-a" "job_master"
}

start_job_worker() {
  if [[ -z ${ALLUXIO_JOB_WORKER_JAVA_OPTS} ]] ; then
    ALLUXIO_JOB_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting job worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   ${ALLUXIO_JOB_WORKER_JAVA_OPTS} \
   alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1) &
  ALLUXIO_JOB_WORKER_JAVA_OPTS+=" -Dalluxio.job.worker.rpc.port=0 -Dalluxio.job.worker.web.port=0"
  local nworkers=${ALLUXIO_JOB_WORKER_COUNT:-1}
  for (( c = 1; c < ${nworkers}; c++ )); do
    echo "Starting job worker #$((c+1)) @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} \
     ${ALLUXIO_JOB_WORKER_JAVA_OPTS} \
     alluxio.worker.AlluxioJobWorker > ${ALLUXIO_LOGS_DIR}/job_worker.out 2>&1) &
  done
}

start_job_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "-a" "job_worker"
}

start_logserver() {
    if [[ ! -d "${ALLUXIO_LOGSERVER_LOGS_DIR}" ]]; then
        echo "ALLUXIO_LOGSERVER_LOGS_DIR: ${ALLUXIO_LOGSERVER_LOGS_DIR}"
        mkdir -p ${ALLUXIO_LOGSERVER_LOGS_DIR}
    fi

    echo "Starting logserver @ $(hostname -f)."
    (nohup "${JAVA}" -cp ${CLASSPATH} \
     ${ALLUXIO_LOGSERVER_JAVA_OPTS} \
     alluxio.logserver.AlluxioLogServer "${ALLUXIO_LOGSERVER_LOGS_DIR}" > ${ALLUXIO_LOGS_DIR}/logserver.out 2>&1) &
    # Wait for 1s before starting other Alluxio servers, otherwise may cause race condition
    # leading to connection errors.
    sleep 1
}

start_master() {
  if [[ "$1" == "-f" ]]; then
    ${LAUNCHER} ${BIN}/alluxio format
  fi

  if [[ ${ALLUXIO_MASTER_SECONDARY} == "true" ]]; then
    if [[ -z ${ALLUXIO_SECONDARY_MASTER_JAVA_OPTS} ]]; then
      ALLUXIO_SECONDARY_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
    fi

    # use a default Xmx value for the master
    contains "${ALLUXIO_SECONDARY_MASTER_JAVA_OPTS}" "Xmx"
    if [[ $? -eq 0 ]]; then
      ALLUXIO_SECONDARY_MASTER_JAVA_OPTS+=" -Xmx8g "
    fi

    echo "Starting secondary master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup "${JAVA}" -cp ${CLASSPATH} \
     ${ALLUXIO_SECONDARY_MASTER_JAVA_OPTS} \
     alluxio.master.AlluxioSecondaryMaster > ${ALLUXIO_LOGS_DIR}/secondary_master.out 2>&1) &
  else
    if [[ -z ${ALLUXIO_MASTER_JAVA_OPTS} ]]; then
      ALLUXIO_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
    fi
    if [[ -n ${journal_backup} ]]; then
      ALLUXIO_MASTER_JAVA_OPTS+=" -Dalluxio.master.journal.init.from.backup=${journal_backup}"
    fi

    # use a default Xmx value for the master
    contains "${ALLUXIO_MASTER_JAVA_OPTS}" "Xmx"
    if [[ $? -eq 0 ]]; then
      ALLUXIO_MASTER_JAVA_OPTS+=" -Xmx8g "
    fi

    echo "Starting master @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup "${JAVA}" -cp ${CLASSPATH} \
     ${ALLUXIO_MASTER_JAVA_OPTS} \
     alluxio.master.AlluxioMaster > ${ALLUXIO_LOGS_DIR}/master.out 2>&1) &
  fi
}

start_masters() {
  start_opts=""
  if [[ -n ${journal_backup} ]]; then
    start_opts="-i ${journal_backup}"
  fi
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-start.sh" ${start_opts} "-a" "master" $1
}

start_proxy() {
  if [[ -z ${ALLUXIO_PROXY_JAVA_OPTS} ]]; then
    ALLUXIO_PROXY_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting proxy @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup "${JAVA}" -cp ${CLASSPATH} \
   ${ALLUXIO_PROXY_JAVA_OPTS} \
   alluxio.proxy.AlluxioProxy > ${ALLUXIO_LOGS_DIR}/proxy.out 2>&1) &
}

start_proxies() {
  ${LAUNCHER} "${BIN}/alluxio-masters.sh" "${BIN}/alluxio-start.sh" "-a" "proxy"
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "-a" "proxy"
}

start_worker() {
  do_mount $1
  if  [ ${MOUNT_FAILED} -ne 0 ] ; then
    echo "Mount failed, not starting worker" >&2
    exit 1
  fi

  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  # use a default Xmx value for the worker
  contains "${ALLUXIO_WORKER_JAVA_OPTS}" "Xmx"
  if [[ $? -eq 0 ]]; then
    ALLUXIO_WORKER_JAVA_OPTS+=" -Xmx4g "
  fi

  # use a default MaxDirectMemorySize value for the worker
  contains "${ALLUXIO_WORKER_JAVA_OPTS}" "XX:MaxDirectMemorySize"
  if [[ $? -eq 0 ]]; then
    ALLUXIO_WORKER_JAVA_OPTS+=" -XX:MaxDirectMemorySize=4g "
  fi

  echo "Starting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup "${JAVA}" -cp ${CLASSPATH} \
   ${ALLUXIO_WORKER_JAVA_OPTS} \
   alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1 ) &
}

start_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "-a" "worker" $1
}

restart_worker() {
  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  RUN=$(ps -ef | grep "alluxio.worker.AlluxioWorker" | grep "java" | wc | awk '{ print $1; }')
  if [[ ${RUN} -eq 0 ]]; then
    echo "Restarting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup "${JAVA}" -cp ${CLASSPATH} \
     ${ALLUXIO_WORKER_JAVA_OPTS} \
     alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1) &
  fi
}

restart_workers() {
  ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "restart_worker"
}

get_offline_worker() {
  local run=
  local result=""
  run=$(ps -ef | grep "alluxio.worker.AlluxioWorker" | grep "java" | wc | awk '{ print $1; }')
  if [[ ${run} -eq 0 ]]; then
    result=$(hostname -f)
  fi
  echo "${result}"
}

get_offline_workers() {
  local result=""
  local run=
  local i=0
  local workers=$(cat "${ALLUXIO_CONF_DIR}/workers" | sed  "s/#.*$//;/^$/d")
  for worker in $(echo ${workers}); do
    if [[ ${i} -gt 0 ]]; then
      result+=","
    fi
    run=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt ${worker} \
        ps -ef | grep "alluxio.worker.AlluxioWorker" | grep "java" | wc | awk '{ print $1; }')
    if [[ ${run} -eq 0 ]]; then
      result+="${worker}"
    fi
    i=$((i+1))
  done
  echo "${result}"
}

start_monitor() {
  local action=$1
  local nodes=$2
  local run=
  if [[ "${action}" == "restart_worker" ]]; then
    action="worker"
    if [[ -z "${nodes}" ]]; then
      run="false"
    fi
  elif [[ "${action}" == "restart_workers" ]]; then
    action="workers"
    if [[ -z "${nodes}" ]]; then
      run="false"
    fi
  elif [[ "${action}" == "logserver" || "${action}" == "safe" ]]; then
    echo -e "Error: Invalid Monitor ACTION: ${action}" >&2
    exit 1
  fi
  if [[ -z "${run}" ]]; then
    ${LAUNCHER} "${BIN}/alluxio-monitor.sh" "${action}" "${nodes}"
  else
    echo "Skipping the monitor checks..."
  fi
}

run_safe() {
  while [ 1 ]
  do
    RUN=$(ps -ef | grep "alluxio.master.AlluxioMaster" | grep "java" | wc | awk '{ print $1; }')
    if [[ ${RUN} -eq 0 ]]; then
      echo "Restarting the system master..."
      start_master
    fi
    echo "Alluxio is running... "
    sleep 2
  done
}

main() {
  # get environment
  get_env

  # ensure log/data dirs
  ensure_dirs

  while getopts "ahNwi:" o; do
    case "${o}" in
      a)
        async="true"
        ;;
      h)
        echo -e "${USAGE}"
        exit 0
        ;;
      i)
        journal_backup=${OPTARG}
        ;;
      N)
        killonstart="no"
        ;;
      w)
        wait="true"
        ;;
      *)
        echo -e "${USAGE}" >&2
        exit 1
        ;;
    esac
  done

  shift $((${OPTIND} - 1))

  ACTION=$1
  if [[ -z "${ACTION}" ]]; then
    echo "Error: no ACTION specified" >&2
    echo -e "${USAGE}" >&2
    exit 1
  fi
  shift

  MOPT=$1
  # Set MOPT.
  case "${ACTION}" in
    all|worker|workers|local)
      if [[ -z "${MOPT}" ]]; then
        echo  "Assuming NoMount by default."
        MOPT="NoMount"
      elif [[ "${MOPT}" == "-f" ]]; then
        echo  "Assuming SudoMount given -f option."
        MOPT="SudoMount"
      else
        shift
      fi
      if [[ "${ACTION}" = "worker" ]] || [[ "${ACTION}" = "local" ]]; then
        check_mount_mode "${MOPT}"
      fi
      ;;
    *)
      MOPT=""
      ;;
  esac

  FORMAT=$1
  if [[ ! -z "${FORMAT}" && "${FORMAT}" != "-f" ]]; then
    echo -e "${USAGE}" >&2
    exit 1
  fi

  MONITOR_NODES=
  if [[ ! "${async}" ]]; then
    case "${ACTION}" in
      restart_worker)
        MONITOR_NODES=$(get_offline_worker)
        ;;
      restart_workers)
        MONITOR_NODES=$(get_offline_workers)
        ;;
      *)
        MONITOR_NODES=""
      ;;
    esac
  fi

  if [[ "${killonstart}" != "no" ]]; then
    case "${ACTION}" in
      all | local | master | masters | secondary_master | job_master | job_masters | proxy | proxies | worker | workers | job_worker | job_workers | logserver)
        stop ${ACTION}
        sleep 1
        ;;
    esac
  fi

  case "${ACTION}" in
    all)
      start_masters "${FORMAT}"
      start_job_masters
      sleep 2
      start_workers "${MOPT}"
      start_job_workers
      start_proxies
      ;;
    local)
      start_master "${FORMAT}"
      ALLUXIO_MASTER_SECONDARY=true
      # We only start a secondary master when using a UFS journal.
      local journal_type=$(${BIN}/alluxio getConf ${ALLUXIO_MASTER_JAVA_OPTS} \
                           alluxio.master.journal.type | awk '{print toupper($0)}')
      if [[ ${journal_type} == "UFS" ]]; then
          start_master
      fi
      ALLUXIO_MASTER_SECONDARY=false
      start_job_master
      sleep 2
      start_worker "${MOPT}"
      start_job_worker
      start_proxy
      ;;
    job_master)
      start_job_master
      ;;
    job_masters)
      start_job_masters
      ;;
    job_worker)
      start_job_worker
      ;;
    job_workers)
      start_job_workers
      ;;
    master)
      start_master "${FORMAT}"
      ;;
    secondary_master)
      ALLUXIO_MASTER_SECONDARY=true
      start_master
      ALLUXIO_MASTER_SECONDARY=false
      ;;
    masters)
      start_masters
      ;;
    proxy)
      start_proxy
      ;;
    proxies)
      start_proxies
      ;;
    restart_worker)
      restart_worker
      ;;
    restart_workers)
      restart_workers
      ;;
    safe)
      run_safe
      ;;
    worker)
      start_worker "${MOPT}"
      ;;
    workers)
      start_workers "${MOPT}"
      ;;
    logserver)
      start_logserver
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}" >&2
    echo -e "${USAGE}" >&2
    exit 1
  esac
  sleep 2

  if [[ "${wait}" ]]; then
    wait
  fi

  if [[ ! "${async}" ]]; then
    start_monitor "${ACTION}" "${MONITOR_NODES}"
  fi
}

main "$@"
