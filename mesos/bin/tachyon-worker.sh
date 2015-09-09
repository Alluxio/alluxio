#!/bin/bash
#
# Usage:
#  tachyon-worker.sh <master-hostname> <memory-allocation>

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"
MASTER_HOSTNAME="$1"
WORKER_MEMOORY_SIZE="$2"

mkdir -p "${TACHYON_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dlog4j.configuration=file:"${TACHYON_CONF_DIR}/log4j.properties" \
  -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="WORKER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dtachyon.master.hostname="${MASTER_HOSTNAME}" \
  -Dtachyon.worker.memory.size="${WORKER_MEMORY_SIZE}" \
  ${TACHYON_WORKER_JAVA_OPTS} tachyon.mesos.TachyonWorkerExecutor > "${TACHYON_LOGS_DIR}"/worker.out 2>&1
