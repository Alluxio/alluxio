#!/bin/bash

#SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
#source "${SCRIPT_DIR}/common.sh"
#TACHYON_MASTER_JAVA_OPTS="${TACHYON_MASTER_JAVA_OPTS:-${TACHYON_JAVA_OPTS}}"

echo $CLASSPATH
echo $TACHYON_HOME
echo $TACHYON_LOGS_DIR
echo reset LOGS_DIR
TACHYON_LOGS_DIR=/tmp/logs
mkdir -p "${TACHYON_LOGS_DIR}"

echo "pwd"
pwd

echo "ls -al -R ."
ls -al -R .

echo "Formatting Tachyon Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.Format master > "${TACHYON_LOGS_DIR}"/master.out 2>&1

echo "Starting Tachyon Master"

echo "${JAVA}" -cp "${CLASSPATH}" ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.master.TachyonMaster

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_MASTER_JAVA_OPTS} \
  -Dtachyon.accesslogger.type="MASTER_ACCESS_LOGGER" \
  -Dtachyon.home="${TACHYON_HOME}" \
  -Dtachyon.logger.type="MASTER_LOGGER" \
  -Dtachyon.logs.dir="${TACHYON_LOGS_DIR}" \
  tachyon.master.TachyonMaster >> "${TACHYON_LOGS_DIR}"/master.out 2>&1
