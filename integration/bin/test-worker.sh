#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_WORKER_JAVA_OPTS="${ALLUXIO_WORKER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

mkdir -p "${ALLUXIO_LOGS_DIR}"

# Format the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_ADDRESS}" \
  -Dalluxio.logger.type="USER_LOGGER" \
  ${ALLUXIO_JAVA_OPTS} "alluxio.Format" "worker"

# Start the worker.
"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS} \
  -Dalluxio.accesslogger.type="WORKER_ACCESS_LOGGER" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  alluxio.mesos.TestWorkerExecutor > "${ALLUXIO_LOGS_DIR}"/test-worker.out 2>&1
