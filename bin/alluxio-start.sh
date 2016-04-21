#!/usr/bin/env bash

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$0" )"; pwd)

#start up alluxio

USAGE="Usage: alluxio-start.sh [-hNw] ACTION [MOPT] [-f]
Where ACTION is one of:
  all [MOPT]\t\tStart master and all workers.
  local [MOPT] \t\t\tStart a master and worker locally.
  master\t\tStart the master on this node.
  safe\t\t\tScript will run continuously and start the master if it's not running.
  worker [MOPT]\t\tStart a worker on this node.
  workers [MOPT]\t\tStart workers on worker nodes.
  restart_worker\tRestart a failed worker on this node.
  restart_workers\tRestart any failed workers on worker nodes.
MOPT is one of:
  Mount\t\t\tMount the configured RamFS. Notice: this will format the existing RamFS.
  SudoMount\t\tMount the configured RamFS using sudo. Notice: this will format the existing RamFS.
  NoMount\t\tDo not mount the configured RamFS. Notice: Use NoMount (Linux only) to use tmpFS
  to avoid sudo requirement.
  SudoMount is assumed if MOPT is missing.

-f  format Journal, UnderFS Data and Workers Folder on master

-N  do not try to kill prior running masters and/or workers in all or local

-w  wait for processes to end before returning

-h  display this help."

ensure_dirs() {
  if [[ ! -d "${ALLUXIO_LOGS_DIR}" ]]; then
    echo "ALLUXIO_LOGS_DIR: ${ALLUXIO_LOGS_DIR}"
    mkdir -p ${ALLUXIO_LOGS_DIR}
  fi
}

get_env() {
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh
}

check_mount_mode() {
  case "$1" in
    Mount);;
    SudoMount);;
    NoMount)
      mount | grep "${ALLUXIO_RAM_FOLDER}" > /dev/null
      if [[ $? -ne 0 || -z ${ALLUXIO_RAM_FOLDER} ]]; then
        if [[ $(uname -s) == Darwin ]]; then
          # Assuming Mac OS X
          echo "ERROR: NoMount is not supported on Mac OS X."
          echo -e "${USAGE}"
          exit 1
        else
          echo "WARNING: Overriding ALLUXIO_RAM_FOLDER to /dev/shm to use tmpFS now."
          export ALLUXIO_RAM_FOLDER="/dev/shm"
          # Set env again since some env variables depend on ALLUXIO_RAM_FOLDER.
          get_env
        fi
      fi
      if [[ "${ALLUXIO_RAM_FOLDER}" =~ ^"/dev/shm"\/{0,1}$ ]]; then
        echo "WARNING: Using tmpFS which is not guaranteed to be in memory."
        echo "WARNING: Check vmstat for memory statistics (e.g. swapping)."
      fi
    ;;
    *)
      if [[ -z $1 ]]; then
        echo "This command requires a mount mode be specified"
      else
        echo "Invalid mount mode: $1"
      fi
      echo -e "${USAGE}"
      exit 1
  esac
}

# pass mode as $1
do_mount() {
  MOUNT_FAILED=0
  case "$1" in
    Mount|SudoMount)
      ${LAUNCHER} ${BIN}/alluxio-mount.sh $1
      MOUNT_FAILED=$?
      ;;
    NoMount)
      ;;
    *)
      echo "This command requires a mount mode be specified"
      echo -e "${USAGE}"
      exit 1
  esac
}

stop() {
  ${BIN}/alluxio-stop.sh all
}


start_master() {
  MASTER_ADDRESS=${ALLUXIO_MASTER_ADDRESS}
  if [[ -z ${ALLUXIO_MASTER_ADDRESS} ]]; then
    MASTER_ADDRESS=localhost
  fi

  if [[ -z ${ALLUXIO_MASTER_JAVA_OPTS} ]]; then
    ALLUXIO_MASTER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  if [[ "$1" == "-f" ]]; then
    ${LAUNCHER} ${BIN}/alluxio format
  fi

  echo "Starting master @ ${MASTER_ADDRESS}. Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   -Dalluxio.home=${ALLUXIO_HOME} \
   -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} \
   -Dalluxio.logger.type="MASTER_LOGGER" \
   -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties \
   ${ALLUXIO_MASTER_JAVA_OPTS} \
   alluxio.master.AlluxioMaster > ${ALLUXIO_LOGS_DIR}/master.out 2>&1) &
}

start_worker() {
  do_mount $1
  if  [ ${MOUNT_FAILED} -ne 0 ] ; then
    echo "Mount failed, not starting worker"
    exit 1
  fi

  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  echo "Starting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
  (nohup ${JAVA} -cp ${CLASSPATH} \
   -Dalluxio.home=${ALLUXIO_HOME} \
   -Dalluxio.logs.dir=${ALLUXIO_LOGS_DIR} \
   -Dalluxio.logger.type="WORKER_LOGGER" \
   -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties \
   ${ALLUXIO_WORKER_JAVA_OPTS} \
   alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1 ) &
}

restart_worker() {
  if [[ -z ${ALLUXIO_WORKER_JAVA_OPTS} ]]; then
    ALLUXIO_WORKER_JAVA_OPTS=${ALLUXIO_JAVA_OPTS}
  fi

  RUN=$(ps -ef | grep "alluxio.worker.AlluxioWorker" | grep "java" | wc | cut -d" " -f7)
  if [[ ${RUN} -eq 0 ]]; then
    echo "Restarting worker @ $(hostname -f). Logging to ${ALLUXIO_LOGS_DIR}"
    (nohup ${JAVA} -cp ${CLASSPATH} \
     -Dalluxio.home=${ALLUXIO_HOME} \
     -Dalluxio.logs \
     .dir=${ALLUXIO_LOGS_DIR} \
     -Dalluxio.logger.type="WORKER_LOGGER" \
     .type="WORKER_ACCESS_LOGGER" \
     -Dlog4j.configuration=file:${ALLUXIO_CONF_DIR}/log4j.properties \
     ${ALLUXIO_WORKER_JAVA_OPTS} \
     alluxio.worker.AlluxioWorker > ${ALLUXIO_LOGS_DIR}/worker.out 2>&1) &
  fi
}

run_safe() {
  while [ 1 ]
  do
    RUN=$(ps -ef | grep "alluxio.master.AlluxioMaster" | grep "java" | wc | cut -d" " -f7)
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

  while getopts "hNw" o; do
    case "${o}" in
      h)
        echo -e "${USAGE}"
        exit 0
        ;;
      N)
        killonstart="no"
        ;;
      w)
        wait="true"
        ;;
      *)
        echo -e "${USAGE}"
        exit 1
        ;;
    esac
  done

  shift $((${OPTIND} - 1))

  ACTION=$1
  if [[ -z "${ACTION}" ]]; then
    echo "Error: no ACTION specified"
    echo -e "${USAGE}"
    exit 1
  fi
  shift

  MOPT=$1
  # Set MOPT.
  case "${ACTION}" in
    all|worker|workers|local)
      if [[ -z "${MOPT}" || "${MOPT}" == "-f" ]]; then
        MOPT="SudoMount"
      else
        shift
      fi
      check_mount_mode "${MOPT}"
      ;;
    *)
      MOPT=""
      ;;
  esac

  FORMAT=$1
  if [[ ! -z "${FORMAT}" && "${FORMAT}" != "-f" ]]; then
    echo -e "${USAGE}"
    exit 1
  fi

  case "${ACTION}" in
    all)
      if [[ "${killonstart}" != "no" ]]; then
        stop ${BIN}
      fi
      start_master "${FORMAT}"
      sleep 2

      ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "worker" "${MOPT}"
      ;;
    local)
      if [[ "${killonstart}" != "no" ]]; then
        stop ${BIN}
        sleep 1
      fi
      start_master "${FORMAT}"
      sleep 2
      start_worker "${MOPT}"
      ;;
    master)
      start_master "${FORMAT}"
      ;;
    worker)
      start_worker "${MOPT}"
      ;;
    safe)
      run_safe
      ;;
    workers)
      ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" "worker" "${MOPT}" \
       "${ALLUXIO_MASTER_ADDRESS}"
      ;;
    restart_worker)
      restart_worker
      ;;
    restart_workers)
      ${LAUNCHER} "${BIN}/alluxio-workers.sh" "${BIN}/alluxio-start.sh" restart_worker
      ;;
    *)
    echo "Error: Invalid ACTION: ${ACTION}"
    echo -e "${USAGE}"
    exit 1
  esac
  sleep 2

  if [[ "${wait}" ]]; then
    wait
  fi
}

main "$@"

