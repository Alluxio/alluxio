#!/usr/bin/env bash
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

set -e

readonly SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)
readonly GENERATE_TARBALL_SCRIPT="${SCRIPT_DIR}/generate-tarball.sh"
readonly HADOOP_PROFILES=( "default" "hadoop-1" "hadoop-2.2" "hadoop-2.3" "hadoop-2.4" "hadoop-2.5" "hadoop-2.6" "hadoop-2.7" "hadoop-2.8" )

function main {
  for hadoop_profile in "${HADOOP_PROFILES[@]}"; do
    echo "building tarball for ${hadoop_profile}"
    "${GENERATE_TARBALL_SCRIPT}" --deleteUnrevisioned --hadoopProfile ${hadoop_profile}
  done
}

main "$@"
