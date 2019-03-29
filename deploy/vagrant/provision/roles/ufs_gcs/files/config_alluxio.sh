#!/bin/bash

if [[ ${ALLUXIO_VERSION_LESSTHAN_1_1} == true ]]; then
  cat >> ~/.bashrc << EOF
export ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="gs://${GCS_BUCKET}"

export ALLUXIO_JAVA_OPTS+="
  -Dfs.gcs.accessKeyId=${GCS_ID}
  -Dfs.gcs.secretAccessKey=${GCS_KEY}
"
EOF
else
  cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="gs://${GCS_BUCKET}"

ALLUXIO_JAVA_OPTS+="
  -Dfs.gcs.accessKeyId=${GCS_ID}
  -Dfs.gcs.secretAccessKey=${GCS_KEY}
"
EOF
fi
