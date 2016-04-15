#!/bin/bash

# create alluxio-env.sh
/alluxio/bin/alluxio bootstrap-conf $(tail -n1 /alluxio/conf/workers)
