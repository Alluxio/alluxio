#!/usr/bin/env bash

ALLUXIO_SITE=/alluxio/conf/alluxio-site.properties
if [[ ${ALLUXIO_VERSION_LESSTHAN_0_8} == true ]]; then
  echo "alluxio.usezookeeper=true" >> "$ALLUXIO_SITE"
else
  echo "alluxio.zookeeper.enabled=true" >> "$ALLUXIO_SITE"
fi

echo "alluxio.zookeeper.address=AlluxioMaster:2181" >> "$ALLUXIO_SITE"
