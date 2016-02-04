#!/bin/bash

sed -i "s|^export TACHYON_UNDERFS_ADDRESS=.*|export TACHYON_UNDERFS_ADDRESS=s3n://${S3_BUCKET}|g" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsSecretAccessKey=${S3_KEY}
" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsAccessKeyId=${S3_ID}
" /tachyon/conf/tachyon-env.sh

# For Tachyon version earlier than 0.8, remove schema "s3n" from default prefixes to be handled by HdfsUnderFileSystem.
# This property is changed to "alluxio.underfs.hdfs.prefixes" after version 0.8 and s3n is not included by default.
PREFIXES=$(grep tachyon.underfs.hadoop.prefixes /tachyon/common/src/main/resources/tachyon-default.properties)
if [[ "$PREFIXES" != "" ]]; then
  PREFIXES=$(echo $PREFIXES | sed -i "s|s3n://,||g")
  # After this change, only S3UnderFileSystem will support s3n://
  sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
    -D${PREFIXES}
  " /tachyon/conf/tachyon-env.sh
fi
