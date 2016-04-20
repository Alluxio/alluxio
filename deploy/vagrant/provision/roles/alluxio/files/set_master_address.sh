#!/bin/bash

# delete any existing alluxio-env.sh
rm -f /alluxio/conf/alluxio-env.sh

# create alluxio-env.sh
/alluxio/bin/alluxio bootstrap-conf $(tail -n1 /alluxio/conf/workers)
