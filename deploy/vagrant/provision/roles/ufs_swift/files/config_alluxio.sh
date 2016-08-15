#!/bin/bash

cat >> ~/.bashrc << EOF
export ALLUXIO_UNDERFS_ADDRESS="swift://${SWIFT_CONTAINER}"

export ALLUXIO_JAVA_OPTS+="
  -Dfs.swift.user=${SWIFT_USER}
  -Dfs.swift.tenant=${SWIFT_TENANT}
  -Dfs.swift.password=${SWIFT_PASSWORD}
  -Dfs.swift.auth.url=${SWIFT_AUTH_URL}
  -Dfs.swift.use.public.url=${SWIFT_USE_PUBLIC_URL}
  -Dfs.swift.auth.method=${SWIFT_AUTH_METHOD}
"
EOF
