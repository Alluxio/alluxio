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

if [[ $# -ne 1 ]]; then
  echo 'expected one argument: "master", "worker", or "proxy"'
  exit 1
fi

service=$1

# Only set ALLUXIO_RAM_FOLDER if tiered storage isn't explicitly configured
if [[ -z "${ALLUXIO_WORKER_TIEREDSTORE_LEVEL0_DIRS_PATH}" ]]; then
  # Docker will set this tmpfs up by default. Its size is configurable through the
  # --shm-size argument to docker run
  export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-/dev/shm}
fi

home=/opt/alluxio
cd ${home}

# List of environment variables which go in alluxio-env.sh instead of
# alluxio-site.properties
alluxio_env_vars=(
  ALLUXIO_CLASSPATH
  ALLUXIO_HOSTNAME
  ALLUXIO_JARS
  ALLUXIO_JAVA_OPTS
  ALLUXIO_MASTER_JAVA_OPTS
  ALLUXIO_PROXY_JAVA_OPTS
  ALLUXIO_RAM_FOLDER
  ALLUXIO_USER_JAVA_OPTS
  ALLUXIO_WORKER_JAVA_OPTS
)

# Map of environment variables that violate the "_ to ."-rule
declare -A alluxio_env_violators=(
  ["ALLUXIO_UNDERFS_S3A_INHERIT_ACL"]="alluxio.underfs.s3a.inherit_acl"
  ["ALLUXIO_MASTER_FORMAT_FILE_PREFIX"]="alluxio.master.format.file_prefix"
)

for keyvaluepair in $(env | grep "ALLUXIO_"); do
  # split around the "="
  key=$(echo ${keyvaluepair} | cut -d= -f1)
  value=$(echo ${keyvaluepair} | cut -d= -f2)
  if [[ "${alluxio_env_vars[*]}" =~ "${key}" ]]; then
    echo "export ${key}=${value}" >> conf/alluxio-env.sh
  else
    if [[ "${!alluxio_env_violators[*]}" =~ "${key}" ]]; then
      echo "${alluxio_env_violators[${key}]}=${value}" >> conf/alluxio-site.properties
    else
      confkey=$(echo ${key} | sed "s/_/./g" | tr '[:upper:]' '[:lower:]')
      echo "${confkey}=${value}" >> conf/alluxio-site.properties
    fi
  fi
done

case ${service,,} in
  master)
    bin/alluxio format
    integration/docker/bin/alluxio-master.sh
    ;;
  worker)
    bin/alluxio formatWorker
    integration/docker/bin/alluxio-worker.sh
    ;;
  proxy)
    integration/docker/bin/alluxio-proxy.sh
    ;;
  *)
    echo 'expected "master", "worker", or "proxy"';
    exit 1
    ;;
esac
