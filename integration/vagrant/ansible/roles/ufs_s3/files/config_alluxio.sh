#!/bin/bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#


cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="s3a://${S3_BUCKET}"

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
