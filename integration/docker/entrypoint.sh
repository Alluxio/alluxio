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

function printUsage {
  echo "Usage: COMMAND [COMMAND_OPTIONS]"
  echo
  echo "COMMAND is one of:"
  echo -e " master [--no-format]    \t Start Alluxio master. If --no-format is specified, do not format"
  echo -e " worker [--no-format]    \t Start Alluxio worker. If --no-format is specified, do not format"
  echo -e " proxy                   \t Start Alluxio proxy"
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
)

for keyvaluepair in $(env); do
  # split around the "="
  key=$(echo ${keyvaluepair} | cut -d= -f1)
  value=$(echo ${keyvaluepair} | cut -d= -f2-)
  if [[ "${alluxio_env_vars[*]}" =~ "${key}" ]]; then
    echo "export ${key}=${value}" >> conf/alluxio-env.sh
  else
    # check if property name is valid
    if confkey=$(bin/alluxio runClass alluxio.cli.GetConfKey ${key} 2> /dev/null); then
      echo "${confkey}=${value}" >> conf/alluxio-site.properties
    fi
  fi
done

case ${service,,} in
  master)
    if [[ -n ${options} && ${options} != ${NO_FORMAT} ]]; then
      printUsage
      exit 1
    fi
    if [[ ${options} != ${NO_FORMAT} ]]; then
      bin/alluxio formatMaster
    fi
    integration/docker/bin/alluxio-master.sh
    ;;
  worker)
    if [[ -n ${options} && ${options} != ${NO_FORMAT} ]]; then
      printUsage
      exit 1
    fi
    if [[ ${options} != ${NO_FORMAT} ]]; then
      bin/alluxio formatWorker
    fi
    integration/docker/bin/alluxio-worker.sh
    ;;
  proxy)
    integration/docker/bin/alluxio-proxy.sh
    ;;
  *)
    printUsage
    exit 1
    ;;
esac
