#!/usr/bin/env bash
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


set -e

cd /mesos/build
MESOS_LOGS_DIR=/mesos/logs
mkdir -p "${MESOS_LOGS_DIR}"
./bin/mesos-slave.sh --work_dir=/mesos/workdir/slave --master=AlluxioMaster:50050 > "${MESOS_LOGS_DIR}/slave.out" 2 > &1 &
