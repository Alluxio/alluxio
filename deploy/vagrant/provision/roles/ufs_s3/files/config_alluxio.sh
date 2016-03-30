#!/bin/bash

sed -i "s|^export ALLUXIO_UNDERFS_ADDRESS=.*|export ALLUXIO_UNDERFS_ADDRESS=s3n://${S3_BUCKET}|g" /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsSecretAccessKey=${S3_KEY}" /alluxio/conf/alluxio-env.sh

sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsAccessKeyId=${S3_ID}" /alluxio/conf/alluxio-env.sh

# For Alluxio version earlier than 0.8, remove schema "s3n" from default prefixes to be handled by HdfsUnderFileSystem.
# This property is changed to "alluxio.underfs.hdfs.prefixes" after version 0.8 and s3n is not included by default.
PREFIXES=$(grep alluxio.underfs.hadoop.prefixes /alluxio/common/src/main/resources/alluxio-default.properties)
if [[ "$PREFIXES" != "" ]]; then
  PREFIXES=$(echo ${PREFIXES} | sed -i "s|s3n://,||g")
  # After this change, only S3UnderFileSystem will support s3n://
  sed -i "/export ALLUXIO_JAVA_OPTS+=\"/ a\
  -D${PREFIXES}" /alluxio/conf/alluxio-env.sh
fi
