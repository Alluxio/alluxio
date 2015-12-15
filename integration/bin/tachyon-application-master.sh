#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"

echo "Launching Application Master"

"${JAVA}" -cp "${CLASSPATH}" \
  ${TACHYON_JAVA_OPTS} \
  -Xmx256M \
  tachyon.yarn.ApplicationMaster $@
