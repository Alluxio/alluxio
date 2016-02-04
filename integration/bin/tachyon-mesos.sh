#!/bin/bash
#
# Usage:
#  alluxio-mesos.sh <mesos-master-hostname>

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_FRAMEWORK_JAVA_OPTS="${ALLUXIO_FRAMEWORK_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

Usage="Usage: alluxio-mesos.sh [-hw] MESOS_MASTER_ADDRESS
+MESOS_MASTER_ADDRESS is of the form 'mesos.example.com:5050'
+
+-w  wait for process to finish before returning
+
+-h  display this help."

while getopts "hw" o; do
  case "${o}" in
    h)
      echo -e "${Usage}"
      exit 0
      ;;
    w)
      wait="true"
      ;;
    *)
      echo -e "${Usage}"
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

MESOS_MASTER_ADDRESS="$1"
if [[ ! "${MESOS_MASTER_ADDRESS}" ]]; then
  echo -e "${Usage}"
  exit 1
fi

mkdir -p "${ALLUXIO_LOGS_DIR}"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_FRAMEWORK_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  alluxio.mesos.TachyonFramework "${MESOS_MASTER_ADDRESS}" > "${ALLUXIO_LOGS_DIR}"/framework.out 2>&1 &

if [[ "${wait}" ]]; then
  wait
fi
