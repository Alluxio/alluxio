#!/bin/bash

cat >> ~/.bashrc << EOF
export ALLUXIO_UNDERFS_ADDRESS="gs://${GCS_BUCKET}"

export ALLUXIO_JAVA_OPTS+="
  -Dfs.gcs.accessKeyId=${GCS_ID}
  -Dfs.gcs.secretAccessKey=${GCS_KEY}
"
EOF
