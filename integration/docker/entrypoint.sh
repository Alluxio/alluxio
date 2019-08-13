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

NO_FORMAT='--no-format'
FUSE_OPTS='--fuse-opts'

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

if [[ $# -lt 1 ]]; then
  printUsage
  exit 1
fi

service=$1
options=$2

# Only set ALLUXIO_RAM_FOLDER if tiered storage isn't explicitly configured
if [[ -z "${ALLUXIO_WORKER_TIEREDSTORE_LEVEL0_DIRS_PATH}" ]]; then
  # Docker will set this tmpfs up by default. Its size is configurable through the
  # --shm-size argument to docker run
  export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-/dev/shm}
fi

home=/opt/alluxio
cd ${home}

# List of environment variables which go in alluxio-env.sh instead of
# alluxio-site.properties
alluxio_env_vars=(
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

function writeConf {
  local IFS=$'\n' # split by line instead of space
  for keyvaluepair in $(env); do
    # split around the first "="
    key=$(echo ${keyvaluepair} | cut -d= -f1)
    value=$(echo ${keyvaluepair} | cut -d= -f2-)
    if [[ "${alluxio_env_vars[*]}" =~ "${key}" ]]; then
      echo "export ${key}=\"${value}\"" >> conf/alluxio-env.sh
    fi
  done
}

writeConf

function disableDNSCache {
  echo "networkaddress.cache.ttl=0" >> $JAVA_HOME/jre/lib/security/java.security
}

disableDNSCache

function formatMasterIfSpecified {
  if [[ -n ${options} && ${options} != ${NO_FORMAT} ]]; then
    printUsage
    exit 1
  fi
  if [[ ${options} != ${NO_FORMAT} ]]; then
    bin/alluxio formatMaster
  fi
}

function formatWorkerIfSpecified {
  if [[ -n ${options} && ${options} != ${NO_FORMAT} ]]; then
    printUsage
    exit 1
  fi
  if [[ ${options} != ${NO_FORMAT} ]]; then
    bin/alluxio formatWorker
  fi
}

function mountAlluxioRootFSWithFuseOption {
  local fuseOptions=""
  if [[ -n ${options} ]]; then
    if [[ ! ${options} =~ ${FUSE_OPTS}=* ]] || [[ ! -n ${options#*=} ]]; then
      printUsage
      exit 1
    fi
    fuseOptions="-o ${options#*=}"
  fi

  # Unmount first if cleanup failed and ignore error
  ! integration/fuse/bin/alluxio-fuse unmount /alluxio-fuse
  integration/fuse/bin/alluxio-fuse mount ${fuseOptions} /alluxio-fuse /
  tail -f /opt/alluxio/logs/fuse.log
}

case ${service,,} in
  master)
    formatMasterIfSpecified
    integration/docker/bin/alluxio-job-master.sh &
    integration/docker/bin/alluxio-master.sh &
    wait -n
    ;;
  master-only)
    formatMasterIfSpecified
    integration/docker/bin/alluxio-master.sh
    ;;
  job-master)
    integration/docker/bin/alluxio-job-master.sh
    ;;
  worker)
    formatWorkerIfSpecified
    integration/docker/bin/alluxio-job-worker.sh &
    integration/docker/bin/alluxio-worker.sh &
    wait -n
    ;;
  worker-only)
    formatWorkerIfSpecified
    integration/docker/bin/alluxio-worker.sh
    ;;
  job-worker)
    integration/docker/bin/alluxio-job-worker.sh
    ;;
  proxy)
    integration/docker/bin/alluxio-proxy.sh
    ;;
  fuse)
    mountAlluxioRootFSWithFuseOption
    ;;
  *)
    printUsage
    exit 1
    ;;
esac
