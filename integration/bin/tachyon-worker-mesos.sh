#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_WORKER_JAVA_OPTS="${ALLUXIO_WORKER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

echo Mount ramdisk on worker
${ALLUXIO_HOME}/bin/tachyon-mount.sh SudoMount

mkdir -p "${ALLUXIO_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS}  \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_ADDRESS}" \
  -Dalluxio.worker.tieredstore.levels=1 \
  -Dalluxio.worker.tieredstore.level0.alias=MEM \
  -Dalluxio.worker.tieredstore.level0.dirs.path="/mnt/ramdisk" \
  -Dalluxio.worker.tieredstore.level0.dirs.quota="${ALLUXIO_WORKER_MEMORY_SIZE}" \
  alluxio.mesos.AlluxioWorkerExecutor > "${ALLUXIO_LOGS_DIR}"/worker.out 2>&1
