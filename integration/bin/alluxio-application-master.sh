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

# Yarn will set $CLASSPATH to point to important Yarn resources, but then
# common.sh will reset $CLASSPATH to point to the jars needed by Alluxio.
# We save $CLASSPATH before calling common.sh, then combine the two.
YARN_CLASSPATH="${CLASSPATH}"
source "${SCRIPT_DIR}/common.sh"
export CLASSPATH="${CLASSPATH}:${YARN_CLASSPATH}"

echo "Launching Application Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_JAVA_OPTS} \
  -Xmx256M \
  alluxio.yarn.ApplicationMaster $@
