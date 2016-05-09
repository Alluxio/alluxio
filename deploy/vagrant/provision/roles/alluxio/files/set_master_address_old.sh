#!/bin/bash

# create alluxio-env.sh
/bin/cp /alluxio/conf/alluxio-env.sh.template /alluxio/conf/alluxio-env.sh

sed -i "s/^export ALLUXIO_MASTER_HOSTNAME=.*/export ALLUXIO_MASTER_HOSTNAME=$(tail -n1 /alluxio/conf/workers)/g" /alluxio/conf/alluxio-env.sh
