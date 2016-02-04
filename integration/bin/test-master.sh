#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_MASTER_JAVA_OPTS="${ALLUXIO_MASTER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

mkdir -p "${ALLUXIO_LOGS_DIR}"

# Format the master.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_ADDRESS}" \
  -Dalluxio.logger.type="USER_LOGGER" \
  ${ALLUXIO_JAVA_OPTS} "alluxio.Format" "master"

# Start the master.
"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  alluxio.mesos.TestMasterExecutor > "${ALLUXIO_LOGS_DIR}"/test-master.out 2>&1
