#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

echo Mount ramdisk on worker
${TACHYON_HOME}/bin/tachyon-mount.sh SudoMount

mkdir -p "${TACHYON_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_WORKER_JAVA_OPTS}  \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="WORKER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dtachyon.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dtachyon.worker.tieredstore.levels=1 \
  -Dtachyon.worker.tieredstore.level0.alias=MEM \
  -Dtachyon.worker.tieredstore.level0.dirs.path="/mnt/ramdisk" \
  -Dtachyon.worker.tieredstore.level0.dirs.quota="${TACHYON_WORKER_MEMORY_SIZE}" \
  tachyon.mesos.TachyonWorkerExecutor > "${TACHYON_LOGS_DIR}"/worker.out 2>&1
