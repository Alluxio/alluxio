#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_WORKER_JAVA_OPTS="${TACHYON_WORKER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

mkdir -p "${TACHYON_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" -Dtachyon.home="${TACHYON_HOME}" -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" -Dtachyon.logger.type="WORKER_LOGGER" -Dtachyon.accesslogger.type="WORKER_ACCESS_LOGGER" -Djava.library.path="${MESOS_LIBRARY_PATH}" -Dlog4j.configuration=file:"${TACHYON_CONF_DIR}/log4j.properties" ${TACHYON_WORKER_JAVA_OPTS} tachyon.mesos.TachyonWorkerExecutor > "${TACHYON_LOGS_DIR}"/worker.out 2>&1
