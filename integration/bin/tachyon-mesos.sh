#!/bin/bash
#
# Usage:
#  tachyon-mesos.sh <mesos-master-hostname>

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
TACHYON_FRAMEWORK_JAVA_OPTS="${TACHYON_FRAMEWORK_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

Usage="Usage: tachyon-mesos.sh [-hF] MESOS_MASTER_ADDRESS
MESOS_MASTER_ADDRESS is of the form 'mesos.example.com:5050'

-F  Run in the foreground

-h  display this help."

run_command() {
  if [ "${run_in_foreground}" = "true" ]; then
    "$@"
  else
    (nohup "$@") &
  fi
}

while getopts "Fh" o; do
  case "${o}" in
    F)
      run_in_foreground="true"
      ;;
    h)
      echo -e "$Usage"
      exit 0
      ;;
    *)
      echo -e "$Usage"
      exit 1
      ;;
  esac
done

shift $((OPTIND-1))

MESOS_MASTER_ADDRESS="$1"
if [ ! "$MESOS_MASTER_ADDRESS" ]; then
  echo -e "$Usage"
  exit 1
fi

mkdir -p "${TACHYON_LOGS_DIR}"


run_command "${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_FRAMEWORK_JAVA_OPTS} \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.mesos.TachyonFramework "${MESOS_MASTER_ADDRESS}" > "${TACHYON_LOGS_DIR}"/framework.out 2>&1
