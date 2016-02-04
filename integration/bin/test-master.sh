#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

# Format the master.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dalluxio.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dalluxio.logger.type="USER_LOGGER" \
  ${TACHYON_JAVA_OPTS} "alluxio.Format" "master"

# Start the master.
"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dalluxio.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  alluxio.mesos.TestMasterExecutor > "${TACHYON_LOGS_DIR}"/test-master.out 2>&1
