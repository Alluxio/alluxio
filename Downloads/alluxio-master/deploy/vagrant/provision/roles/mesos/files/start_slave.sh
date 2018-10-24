#!/usr/bin/env bash

set -e

cd /mesos/build
MESOS_LOGS_DIR=/mesos/logs
mkdir -p "${MESOS_LOGS_DIR}"
./bin/mesos-slave.sh --work_dir=/mesos/workdir/slave --master=AlluxioMaster:50050 > "${MESOS_LOGS_DIR}/slave.out" 2 > &1 &
