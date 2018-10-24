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

source "${SCRIPT_DIR}/alluxio-env-mesos.sh"
source "${SCRIPT_DIR}/common.sh"
MESOS_LIBRARY_PATH="${MESOS_LIBRARY_PATH:-/usr/local/lib}"

echo Mount ramdisk on worker
${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount

mkdir -p "${ALLUXIO_LOGS_DIR}"

"${JAVA}" -cp "${ALLUXIO_SERVER_CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS}  \
  -Djava.library.path="${MESOS_LIBRARY_PATH}" \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${ALLUXIO_LOGS_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_HOSTNAME}" \
  -Dalluxio.worker.tieredstore.level0.dirs.quota="${ALLUXIO_WORKER_MEMORY_SIZE}" \
  alluxio.mesos.AlluxioWorkerExecutor > "${ALLUXIO_LOGS_DIR}"/worker.out 2>&1
