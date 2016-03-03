#!/usr/bin/env bash

set -e

cd /mesos/build
MESOS_LOGS_DIR=/mesos/logs
mkdir -p "${MESOS_LOGS_DIR}"
./bin/mesos-master.sh --work_dir=/mesos/workdir/master --port=50050 > "${MESOS_LOGS_DIR}/master.out" 2 > &1 &
