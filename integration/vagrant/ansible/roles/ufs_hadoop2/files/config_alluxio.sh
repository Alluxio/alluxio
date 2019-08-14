#!/bin/bash

# The last node in /alluxio/conf/workers is the master for under filesystem,
# this is guaranteed by generation process of /alluxio/conf/workers in script vagrant/create.
UFS_MASTER=$(tail -n1 /alluxio/conf/workers)
cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="hdfs://${UFS_MASTER}:9000"
EOF
