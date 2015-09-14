#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

# Format the master.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dtachyon.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dtachyon.logger.type="USER_LOGGER" \
  ${TACHYON_JAVA_OPTS} "tachyon.Format" "master"

# Start the master.
"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.mesos.TestMasterExecutor > "${TACHYON_LOGS_DIR}"/test-master.out 2>&1
