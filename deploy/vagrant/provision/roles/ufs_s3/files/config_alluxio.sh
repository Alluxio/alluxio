#!/bin/bash

if [[ ${ALLUXIO_VERSION_LESSTHAN_1_1} == true ]]; then
  cat >> ~/.bashrc << EOF
export ALLUXIO_UNDERFS_ADDRESS="s3n://${S3_BUCKET}"

export ALLUXIO_JAVA_OPTS+="
  -Dfs.s3n.awsSecretAccessKey=${S3_KEY}
  -Dfs.s3n.awsAccessKeyId=${S3_ID}
"
EOF
else
  if [[ ${ALLUXIO_VERSION_LESSTHAN_1_2} == true ]]; then
    cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_UNDERFS_ADDRESS="s3n://${S3_BUCKET}"

ALLUXIO_JAVA_OPTS+="
  -Dfs.s3n.awsSecretAccessKey=${S3_KEY}
  -Dfs.s3n.awsAccessKeyId=${S3_ID}
"
EOF
  else
    echo "alluxio.underfs.address=s3a://${S3_BUCKET}" >> /alluxio/conf/alluxio-site.properties
    echo "aws.accessKeyId=${S3_ID}" >> /alluxio/conf/alluxio-site.properties
    echo "aws.secretKey=${S3_KEY}" >> /alluxio/conf/alluxio-site.properties
  fi
fi

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
