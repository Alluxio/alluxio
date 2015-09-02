#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"

DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}/../../libexec"
TACHYON_LIBEXEC_DIR="${TACHYON_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}"
. "${TACHYON_LIBEXEC_DIR}/tachyon-config.sh"

MASTER_ADDRESS="${TACHYON_MASTER_ADDRESS:-localhost}"
TACHYON_HOME="${TACHYON_HOME:-/Users/jsimsa/Projects/tachyon}"
TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

mkdir -p "${TACHYON_LOGS_DIR}"

# Format the master.
"${JAVA}" -cp "${CLASSPATH}" -Dtachyon.home="${TACHYON_HOME}" -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" -Dtachyon.master.hostname="${TACHYON_MASTER_ADDRESS}" -Dtachyon.logger.type="USER_LOGGER" ${TACHYON_JAVA_OPTS} "tachyon.Format" "master"

# Start the master.
(nohup "${JAVA}" -cp "${CLASSPATH}" -Dtachyon.home="${TACHYON_HOME}" -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" -Dtachyon.logger.type="MASTER_LOGGER" -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" -Dlog4j.configuration=file:"${TACHYON_CONF_DIR}/log4j.properties" ${TACHYON_MASTER_JAVA_OPTS} tachyon.mesos.TestMasterExecutor > "${TACHYON_LOGS_DIR}"/master.out 2>&1)
