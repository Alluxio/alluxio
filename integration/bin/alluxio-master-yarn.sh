#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_MASTER_JAVA_OPTS="${ALLUXIO_MASTER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

# Yarn will set LOG_DIRS to point to the Yarn application log directory
YARN_LOG_DIR="$LOG_DIRS"

echo "Formatting Alluxio Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.Format master > "${YARN_LOG_DIR}"/master.out 2>&1

echo "Starting Alluxio Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.master.AlluxioMaster >> "${YARN_LOG_DIR}"/master.out 2>&1
