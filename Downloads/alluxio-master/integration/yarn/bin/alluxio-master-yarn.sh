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
ALLUXIO_MASTER_JAVA_OPTS="${ALLUXIO_MASTER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

# Yarn will set LOG_DIRS to point to the Yarn application log directory
YARN_LOG_DIR="$LOG_DIRS"

echo "Formatting Alluxio Master"

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.cli.Format master > "${YARN_LOG_DIR}"/master.out 2>&1

echo "Starting Alluxio Master"

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_MASTER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="MASTER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.master.AlluxioMaster >> "${YARN_LOG_DIR}"/master.out 2>&1
