#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

# ${TACHYON_WORKER_MEMORY_SIZE} needs to be set to mount ramdisk
echo "Mounting ramdisk of ${TACHYON_WORKER_MEMORY_SIZE} MB on Worker"
${TACHYON_HOME}/bin/tachyon-mount.sh SudoMount

mkdir -p "${TACHYON_LOGS_DIR}"

echo "Formatting Tachyon Worker"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_WORKER_JAVA_OPTS} \
  -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties \
  -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="WORKER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.Format WORKER > "${TACHYON_LOGS_DIR}"/worker.out 2>&1

echo "Starting Tachyon Worker"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_WORKER_JAVA_OPTS} \
  -Dlog4j.configuration=file:$TACHYON_CONF_DIR/log4j.properties \
  -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="WORKER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dtachyon.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  tachyon.worker.TachyonWorker >> "${TACHYON_LOGS_DIR}"/worker.out 2>&1
