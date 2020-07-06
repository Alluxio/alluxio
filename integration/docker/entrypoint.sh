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

set -e

ALLUXIO_HOME="/opt/alluxio"
NO_FORMAT='--no-format'
FUSE_OPTS='--fuse-opts'
MOUNT_POINT="${MOUNT_POINT:-/mnt/alluxio-fuse}"

# List of environment variables which go in alluxio-env.sh instead of
# alluxio-site.properties
ALLUXIO_ENV_VARS=(
  ALLUXIO_CLASSPATH
  ALLUXIO_HOSTNAME
  ALLUXIO_JARS
  ALLUXIO_JAVA_OPTS
  ALLUXIO_MASTER_JAVA_OPTS
  ALLUXIO_PROXY_JAVA_OPTS
  ALLUXIO_RAM_FOLDER
  ALLUXIO_USER_JAVA_OPTS
  ALLUXIO_WORKER_JAVA_OPTS
  ALLUXIO_JOB_MASTER_JAVA_OPTS
  ALLUXIO_JOB_WORKER_JAVA_OPTS
)

function printUsage {
  echo "Usage: COMMAND [COMMAND_OPTIONS]"
  echo
  echo "COMMAND is one of:"
  echo -e " master [--no-format]         \t Start Alluxio master. If --no-format is specified, do not format"
  echo -e " master-only [--no-format]    \t Start Alluxio master w/o job master. If --no-format is specified, do not format"
  echo -e " worker [--no-format]         \t Start Alluxio worker. If --no-format is specified, do not format"
  echo -e " worker-only [--no-format]    \t Start Alluxio worker w/o job worker. If --no-format is specified, do not format"
  echo -e " job-master                   \t Start Alluxio job master"
  echo -e " job-worker                   \t Start Alluxio job worker"
  echo -e " proxy                        \t Start Alluxio proxy"
  echo -e " fuse [--fuse-opts=opt1,...]  \t Start Alluxio FUSE file system, option --fuse-opts expects a list of fuse options separated by comma"
}

function writeConf {
  local IFS=$'\n' # split by line instead of space
  for keyvaluepair in $(env); do
    # split around the first "="
    key=$(echo ${keyvaluepair} | cut -d= -f1)
    value=$(echo ${keyvaluepair} | cut -d= -f2-)
    if [[ "${ALLUXIO_ENV_VARS[*]}" =~ "${key}" ]]; then
      echo "export ${key}=\"${value}\"" >> conf/alluxio-env.sh
    fi
  done
}

function formatMasterIfSpecified {
  if [[ -n ${OPTIONS} && ${OPTIONS} != ${NO_FORMAT} ]]; then
    printUsage
    exit 1
  fi
  if [[ ${OPTIONS} != ${NO_FORMAT} ]]; then
    bin/alluxio formatMaster
  fi
}

function formatWorkerIfSpecified {
  if [[ -n ${OPTIONS} && ${OPTIONS} != ${NO_FORMAT} ]]; then
    printUsage
    exit 1
  fi
  if [[ ${OPTIONS} != ${NO_FORMAT} ]]; then
    bin/alluxio formatWorker
  fi
}

function mountAlluxioRootFSWithFuseOption {
  local fuseOptions=""
  if [[ -n ${OPTIONS} ]]; then
    if [[ ! ${OPTIONS} =~ ${FUSE_OPTS}=* ]] || [[ ! -n ${OPTIONS#*=} ]]; then
      printUsage
      exit 1
    fi
    fuseOptions="-o ${OPTIONS#*=}"
  fi

  # Unmount first if cleanup failed and ignore error
  ! mkdir -p ${MOUNT_POINT}
  ! umount ${MOUNT_POINT}
  #! integration/fuse/bin/alluxio-fuse unmount ${MOUNT_POINT}
  exec integration/fuse/bin/alluxio-fuse mount -n ${fuseOptions} ${MOUNT_POINT} /
}

# Sends a signal to each of the running background processes
#
# Args:
#     1: the signal to send
function forward_signal {
  local signal="${1}"
  # background jobs don't respond to SIGINT (2)
  # Change to SIGHUP (1)
  if [ "${signal}" -eq "2" ]; then
    signal="1"
  fi

  local procs="$(jobs -p)"
  echo -e "Forwarding signal ${signal} to processes:\n${procs}"
  while read -r proc; do
    if [ -n "${proc}" ]; then
      kill -${signal} "${proc}"
    fi
  done <<< "${procs}"
  # This function may take over execution thread from the "main" function.
  # Wait if the processes are still up. Additional signals of the same type as this
  # will not be able to be processed
  wait
}

# Sets up traps on all signals [1, 31]
#
# Notes about trapping some signals
# - SIGINT (2): Background process (started with &) ignore SIGINT. As a workaround to still
#               terminate processes when SIGINT is passed, convert the signal sent to the
#               processes to be something other than SIGINT
# - SIGKILL (9): Cannot be trapped. It will directly kill the bash parent shell, the child
#                processes will continue to live
function setup_signals {
  for i in {1..31}; do
    trap "forward_signal ${i}" ${i}
  done

  # If the script exits for any reason without a signal, forward a SIGHUP to the children
  trap "forward_signal 1" EXIT
}

function main {
  if [[ "$#" -lt 1 ]]; then
    printUsage
    exit 1
  fi

  local service="$1"
  OPTIONS="$2"

  # Only set ALLUXIO_RAM_FOLDER if tiered storage isn't explicitly configured
  if [[ -z "${ALLUXIO_WORKER_TIEREDSTORE_LEVEL0_DIRS_PATH}" ]]; then
    # Docker will set this tmpfs up by default. Its size is configurable through the
    # --shm-size argument to docker run
    export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-/dev/shm}
  fi

  cd ${ALLUXIO_HOME}

  writeConf

  local processes
  processes=()
  case "${service}" in
    master)
      formatMasterIfSpecified
      processes+=("job_master")
      processes+=("master")
      ;;
    master-only)
      formatMasterIfSpecified
      processes+=("master")
      ;;
    job-master)
      processes+=("job_master")
      ;;
    worker)
      formatWorkerIfSpecified
      processes+=("job_worker")
      processes+=("worker")
      ;;
    worker-only)
      formatWorkerIfSpecified
      processes+=("worker")
      ;;
    job-worker)
      processes+=("job_worker")
      ;;
    proxy)
      processes+=("proxy")
      ;;
    fuse)
      mountAlluxioRootFSWithFuseOption
      ;;
    *)
      printUsage
      exit 1
      ;;
  esac

  if [ -z "${processes}" ]; then
    printUsage
    exit 1
  fi

  # Only a single process is going to be started, simply exec and replace in the shell
  if [ "${#processes[@]}" -eq 1 ]; then
    exec ./bin/launch-process "${processes[0]}" -c
  fi

  # Multiple processes may be running, so manage them by forwarding any signals to them.
  setup_signals

  for proc in "${processes[@]}"; do
    ./bin/launch-process "${proc}" -c &
  done
  wait
}

main "$@"
