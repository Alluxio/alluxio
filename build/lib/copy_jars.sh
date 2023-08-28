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

# copy_jars.sh is called by various pom.xml files to selectively copy plugin jars to lib/

set -eu

# list of jars to copy that are generated by the pom.xml with the corresponding artifactId
INCLUDED_NAMES=(
  "alluxio-integration-tools-hms"
  "alluxio-integration-tools-validation"
  "alluxio-underfs-abfs"
  "alluxio-underfs-adl"
  "alluxio-underfs-cephfs"
  "alluxio-underfs-cephfs-hadoop"
  "alluxio-underfs-cos"
  "alluxio-underfs-cosn"
  "alluxio-underfs-gcs"
  "alluxio-underfs-hdfs"
  "alluxio-underfs-kodo"
  "alluxio-underfs-local"
  "alluxio-underfs-obs"
  "alluxio-underfs-oss"
  "alluxio-underfs-ozone"
  "alluxio-underfs-s3a"
  "alluxio-underfs-wasb"
)

# list of jars to skip copying that are generated by the pom.xml with the corresponding artifactId
EXCLUDED_NAMES=(
  "alluxio-table-server-underdb-glue"
  "alluxio-table-server-underdb-hive"
)

LIB_JAR_NAME=$1
SRC_PATH=$2
DST_PATH=$3

# if jar name is in included list, copy jar
for INCLUDED in "${INCLUDED_NAMES[@]}"; do
  if [[ "${LIB_JAR_NAME}" == "${INCLUDED}" ]]; then
    cp "${SRC_PATH}" "${DST_PATH}"
    exit 0
  fi
done

# if jar name is in excluded list, exit without error
for EXCLUDED in "${EXCLUDED_NAMES[@]}"; do
  if [[ "${LIB_JAR_NAME}" == "${EXCLUDED}" ]]; then
    exit 0
  fi
done

# jar is not in either list, return error
echo "${LIB_JAR_NAME} is not in either list of included or excluded names"
exit 1
