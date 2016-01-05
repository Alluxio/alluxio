#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

# Yarn will set LOG_DIRS to point to the Yarn application log directory
YARN_LOG_DIR="$LOG_DIRS"

echo "Formatting Tachyon Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${YARN_LOG_DIR}" \
  tachyon.Format master > "${YARN_LOG_DIR}"/master.out 2>&1

echo "Starting Tachyon Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${YARN_LOG_DIR}" \
  tachyon.master.TachyonMaster >> "${YARN_LOG_DIR}"/master.out 2>&1
