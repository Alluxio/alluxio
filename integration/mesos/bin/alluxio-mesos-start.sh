#!/bin/bash
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

#
# Usage:
#  alluxio-mesos-start.sh <mesos-master-hostname>

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_FRAMEWORK_JAVA_OPTS="${ALLUXIO_FRAMEWORK_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

Usage="Usage: alluxio-mesos-start.sh [-hw] MESOS_MASTER_ADDRESS [ALLUXIO_MASTER_HOSTNAME]
MESOS_MASTER_ADDRESS is of the form 'mesos.example.com:5050'

ALLUXIO_MASTER_HOSTNAME can be used to specify the hostname of the Mesos slave to launch the
Alluxio master on

-w  wait for process to finish before returning

-h  display this help."

while getopts "hw" o; do
  case "${o}" in
    h)
      echo -e "${Usage}"
      exit 0
      ;;
    w)
      wait="true"
      ;;
    *)
      echo -e "${Usage}" >&2
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

MESOS_MASTER_ADDRESS="$1"
if [[ ! "${MESOS_MASTER_ADDRESS}" ]]; then
  echo -e "${Usage}" >&2
  exit 1
fi

mkdir -p "${ALLUXIO_LOGS_DIR}"

FRAMEWORK_ARGS="--mesos ${MESOS_MASTER_ADDRESS}"
if [[ -n "$2" ]]; then
  FRAMEWORK_ARGS+=" --alluxio-master $2"
fi

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_FRAMEWORK_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="Console" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  alluxio.mesos.AlluxioFramework ${FRAMEWORK_ARGS} > "${ALLUXIO_LOGS_DIR}"/framework.out 2>&1 &

if [[ "${wait}" ]]; then
  wait
fi
