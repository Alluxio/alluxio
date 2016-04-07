#!/bin/bash

sed -i "s|^export ALLUXIO_UNDERFS_ADDRESS=.*|export ALLUXIO_UNDERFS_ADDRESS=gs://${GCS_BUCKET}|g" /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dfs.gcs.secretAccessKey=${GCS_KEY}" /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dfs.gcs.accessKeyId=${GCS_ID}" /alluxio/conf/alluxio-env.sh
