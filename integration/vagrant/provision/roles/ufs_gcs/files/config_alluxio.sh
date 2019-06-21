#!/bin/bash

cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="gs://${GCS_BUCKET}"

ALLUXIO_JAVA_OPTS+="
  -Dfs.gcs.accessKeyId=${GCS_ID}
  -Dfs.gcs.secretAccessKey=${GCS_KEY}
"
EOF
