#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

# Format the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  -Dtachyon.master.hostname="${TACHYON_MASTER_ADDRESS}" \
  -Dtachyon.logger.type="USER_LOGGER" \
  ${TACHYON_JAVA_OPTS} "tachyon.Format" "worker"

# Start the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_WORKER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="WORKER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.mesos.TestWorkerExecutor > "${TACHYON_LOGS_DIR}"/test-worker.out 2>&1
