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
  -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dalluxio.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dalluxio.worker.tieredstore.levels=1 \
  -Dalluxio.worker.tieredstore.level0.alias=MEM \
  -Dalluxio.worker.tieredstore.level0.dirs.path="/mnt/ramdisk" \
  -Dalluxio.worker.tieredstore.level0.dirs.quota="${TACHYON_WORKER_MEMORY_SIZE}" \
  alluxio.mesos.AlluxioWorkerExecutor > "${TACHYON_LOGS_DIR}"/worker.out 2>&1
