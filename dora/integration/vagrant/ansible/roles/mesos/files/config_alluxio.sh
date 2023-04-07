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

VARIABLES="\
ALLUXIO_MASTER_BIND_HOST \
ALLUXIO_MASTER_WEB_BIND_HOST \
ALLUXIO_WORKER_BIND_HOST \
ALLUXIO_WORKER_DATA_BIND_HOST \
ALLUXIO_WORKER_WEB_BIND_HOST \
ALLUXIO_MASTER_HOSTNAME \
ALLUXIO_MASTER_WEB_HOSTNAME \
ALLUXIO_WORKER_HOSTNAME \
ALLUXIO_WORKER_DATA_HOSTNAME \
ALLUXIO_WORKER_WEB_HOSTNAME"

for var in ${VARIABLES}; do
  sed -i "s/# ${var}/${var}/g" /alluxio/conf/alluxio-env.sh
done
