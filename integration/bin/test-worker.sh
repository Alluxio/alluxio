#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

# Format the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dalluxio.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dalluxio.logger.type="USER_LOGGER" \
  ${TACHYON_JAVA_OPTS} "alluxio.Format" "worker"

# Start the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_WORKER_JAVA_OPTS} \
  -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dalluxio.home="${TACHYON_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${TACHYON_LOGS_DIR}" \
  alluxio.mesos.TestWorkerExecutor > "${TACHYON_LOGS_DIR}"/test-worker.out 2>&1
