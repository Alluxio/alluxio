#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_MASTER_JAVA_OPTS="${ALLUXIO_MASTER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

mkdir -p "${ALLUXIO_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  alluxio.mesos.TachyonMasterExecutor > "${ALLUXIO_LOGS_DIR}"/master.out 2>&1
