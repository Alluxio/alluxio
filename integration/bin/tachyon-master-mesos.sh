#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

mkdir -p "${TACHYON_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  alluxio.mesos.AlluxioMasterExecutor > "${TACHYON_LOGS_DIR}"/master.out 2>&1
