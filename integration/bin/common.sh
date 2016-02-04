#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}/../../libexec"
TACHYON_LIBEXEC_DIR="${TACHYON_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}"
source "${TACHYON_LIBEXEC_DIR}/alluxio-config.sh"
MASTER_ADDRESS="${TACHYON_MASTER_ADDRESS:-localhost}"
TACHYON_HOME="${TACHYON_HOME:-${SCRIPT_DIR}/../..}"