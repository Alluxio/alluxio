#!/bin/bash

cat >> /alluxio/conf/alluxio-env.sh << EOF
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="swift://${SWIFT_CONTAINER}"

ALLUXIO_JAVA_OPTS+="
  -Dfs.swift.user=${SWIFT_USER}
  -Dfs.swift.tenant=${SWIFT_TENANT}
  -Dfs.swift.password=${SWIFT_PASSWORD}
  -Dfs.swift.auth.url=${SWIFT_AUTH_URL}
  -Dfs.swift.auth.method=${SWIFT_AUTH_METHOD}
"
EOF
