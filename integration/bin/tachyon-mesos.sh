#!/bin/bash
#
# Usage:
#  tachyon-mesos.sh <mesos-master-hostname>

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_FRAMEWORK_JAVA_OPTS="${TACHYON_FRAMEWORK_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"
MESOS_MASTER_ADDRESS="$1"
RUN_IN_FOREGROUND=false

mkdir -p "${TACHYON_LOGS_DIR}"

if [ "$RUN_IN_FOREGROUND" = "true" ]; then
  BACKGROUND_ARG=""
else
  BACKGROUND_ARG=&
fi

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_FRAMEWORK_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.mesos.TachyonFramework "${MESOS_MASTER_ADDRESS}" > "${TACHYON_LOGS_DIR}"/framework.out 2>&1 $BACKGROUND_ARG
