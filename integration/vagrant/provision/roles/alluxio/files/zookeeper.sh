#!/usr/bin/env bash

ALLUXIO_SITE=/alluxio/conf/alluxio-site.properties
echo "alluxio.zookeeper.enabled=true" >> "$ALLUXIO_SITE"
echo "alluxio.zookeeper.address=AlluxioMaster:2181" >> "$ALLUXIO_SITE"
