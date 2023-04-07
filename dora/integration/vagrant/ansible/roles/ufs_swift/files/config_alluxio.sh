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
ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="swift://${SWIFT_CONTAINER}"

ALLUXIO_JAVA_OPTS+="
  -Dfs.swift.user=${SWIFT_USER}
  -Dfs.swift.tenant=${SWIFT_TENANT}
  -Dfs.swift.password=${SWIFT_PASSWORD}
  -Dfs.swift.auth.url=${SWIFT_AUTH_URL}
  -Dfs.swift.auth.method=${SWIFT_AUTH_METHOD}
"
EOF
