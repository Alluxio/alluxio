#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
DEFAULT_LIBEXEC_DIR="${SCRIPT_DIR}/../../libexec"
ALLUXIO_LIBEXEC_DIR="${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}"
source "${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh"
# TODO: Remove for v2.0 release, below line in support of backwards compatibility
MASTER_HOSTNAME="${ALLUXIO_MASTER_ADDRESS:-localhost}"
ALLUXIO_MASTER_HOSTNAME="${MASTER_HOSTNAME:-localhost}"
ALLUXIO_HOME="${ALLUXIO_HOME:-${SCRIPT_DIR}/../..}"
