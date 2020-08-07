#!/bin/bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_WORKER_JAVA_OPTS="${ALLUXIO_WORKER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

# ${ALLUXIO_WORKER_RAMDISK_SIZE} needs to be set to mount ramdisk
echo "Mounting ramdisk of ${ALLUXIO_WORKER_RAMDISK_SIZE} MB on Worker"
${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount

# Yarn will set LOG_DIRS to point to the Yarn application log directory
YARN_LOG_DIR="$LOG_DIRS"

echo "Formatting Alluxio Worker"

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.cli.Format WORKER > "${YARN_LOG_DIR}"/worker.out 2>&1

echo "Starting Alluxio Worker"

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_HOSTNAME}" \
  alluxio.worker.AlluxioWorker >> "${YARN_LOG_DIR}"/worker.out 2>&1
