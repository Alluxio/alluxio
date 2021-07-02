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


# The last node in /alluxio/conf/workers is the master for under filesystem,
# this is guaranteed by generation process of /alluxio/conf/workers in script vagrant/create.
UFS_MASTER=$(tail -n1 /alluxio/conf/workers)
cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="hdfs://${UFS_MASTER}:9000"
EOF
