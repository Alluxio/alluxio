#!/bin/bash

cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_UNDERFS_ADDRESS="s3a://${S3_BUCKET}"

ALLUXIO_JAVA_OPTS+="
  -Daws.secretKey=${S3_KEY}
  -Daws.accessKeyId=${S3_ID}
"
EOF

# For Alluxio version earlier than 0.8, remove schema "s3n" from default prefixes to be handled by HdfsUnderFileSystem.
# This property is changed to "alluxio.underfs.hdfs.prefixes" after version 0.8 and s3n is not included by default.
PREFIXES=$(grep alluxio.underfs.hadoop.prefixes /alluxio/common/src/main/resources/alluxio-default.properties)
if [[ "$PREFIXES" != "" ]]; then
  PREFIXES=$(echo ${PREFIXES} | sed -i "s|s3n://,||g")
  # After this change, only S3UnderFileSystem will support s3n://
  cat >> ~/.bashrc << EOF
  export ALLUXIO_JAVA_OPTS+="-D${PREFIXES}"
EOF
fi
