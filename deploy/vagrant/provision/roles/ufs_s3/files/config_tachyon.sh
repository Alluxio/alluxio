#!/bin/bash

sed -i "s|^export TACHYON_UNDERFS_ADDRESS=.*|export TACHYON_UNDERFS_ADDRESS=s3n://${S3_BUCKET}|g" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsSecretAccessKey=${S3_KEY}
" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dfs.s3n.awsAccessKeyId=${S3_ID} 
" /tachyon/conf/tachyon-env.sh

PREFIXES=`grep tachyon.underfs.hadoop.prefixes /tachyon/common/src/main/resources/tachyon-default.properties | sed "s|s3n://,||g"`
# After this change, only S3UnderFileSystem will support s3n://
sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -D${PREFIXES}
" /tachyon/conf/tachyon-env.sh
