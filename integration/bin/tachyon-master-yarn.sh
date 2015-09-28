#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  tachyon.master.TachyonMaster > "${TACHYON_LOGS_DIR}"/master.out 2>&1
