#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"

DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}/../../libexec"
TACHYON_LIBEXEC_DIR="${TACHYON_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}"
. "${TACHYON_LIBEXEC_DIR}/tachyon-config.sh"

MASTER_ADDRESS="${TACHYON_MASTER_ADDRESS:-localhost}"
TACHYON_HOME="${TACHYON_HOME:-/Users/jsimsa/Projects/tachyon}"
TACHYON_FRAMEWORK_JAVA_OPTS="${TACHYON_FRAMEWORK_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"
MESOS_MASTER_ADDRESS="$1"

mkdir -p "${TACHYON_LOGS_DIR}"

(nohup "${JAVA}" -cp "${CLASSPATH}" -Dtachyon.home="${TACHYON_HOME}" -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" -Djava.library.path="${MESOS_LIBRARY_PATH}" ${TACHYON_FRAMEWORK_JAVA_OPTS} tachyon.mesos.TachyonFramework "${MESOS_MASTER_ADDRESS}" > "${TACHYON_LOGS_DIR}"/framework.out 2>&1)
