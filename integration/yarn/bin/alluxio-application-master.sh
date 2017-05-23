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
export CLASSPATH="${ALLUXIO_SERVER_CLASSPATH}:${CLASSPATH}"

echo "Launching Application Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_JAVA_OPTS} \
  -Dalluxio.logger.type=Console \
  -Xmx256M \
  alluxio.yarn.ApplicationMaster $@
