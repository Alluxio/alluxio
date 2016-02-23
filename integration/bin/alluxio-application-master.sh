#!/bin/bash

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
