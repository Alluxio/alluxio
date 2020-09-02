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


DISK=$(ls / | grep '^disk')
TIERED_PATH=""
TIERED_QUOTA=""
for disk in ${DISK}; do
  TIERED_PATH=/${disk},${TIERED_PATH}
  quota=$(df -h | grep "/${disk}" | awk '{print $2}')
  TIERED_QUOTA=${quota}B,${TIERED_QUOTA}
done

[[ "$TIERED_PATH" == "" ]] && exit 0

sed -i "s/alluxio.worker.tieredstore.levels=1/alluxio.worker.tieredstore.levels=2/g
  " /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dalluxio.worker.tieredstore.level1.dirs.quota=$TIERED_QUOTA
  " /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dalluxio.worker.tieredstore.level1.dirs.path=$TIERED_PATH
  " /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dalluxio.worker.tieredstore.level1.alias=SSD
  " /alluxio/conf/alluxio-env.sh
