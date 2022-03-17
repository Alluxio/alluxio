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

# This script assumes the following:
# - Logstash OSS 7.12.1 is being used
# - Clean installation
# - Downloads available here: https://www.elastic.co/downloads/past-releases/logstash-oss-7-12-1

set -eu
# Debugging
# set -x

# Arguments
# $1 Alluxio property key
function get_conf() {
  echo -n $(bin/alluxio getConf ${1})
}

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "${SCRIPTPATH}/../../.." > /dev/null 2>&1

HELP=$(cat <<EOF
Usage: logstash-start.sh [-d] /path/to/logstash

    -d                   Set to disable verifying Elasticsearch ssl certificate. This is primarily used in
                         development/testing environments to connect to locally launched Elasticsearch clusters.
    /path/to/logstash    Path to logstash directory

EOF
)

if [[ "$#" -eq 0 ]]; then
  echo "Arguments /path/to/logstash' must be provided."
  echo "${HELP}"
  exit 1
fi

USE_DEV="false"
while getopts "d" o; do
  case "${o}" in
    d)
      USE_DEV="true"
      shift
      ;;
    *)
      ;;
  esac
done

LOGSTASH_DIR="${1}"

if [[ ! -d ${LOGSTASH_DIR} ]]; then
  echo "Directory ${LOGSTASH_DIR} DOES NOT exists."
  exit 1
fi

if [[ ! -f ${SCRIPTPATH}/env ]]; then
  echo "env DOES NOT exists."
  exit 1
fi

# set environment variables used for logstash.conf
set -a # automatically export all variables
source ${SCRIPTPATH}/env
CONF_CLUSTER_ID=$(get_conf 'alluxio.hub.cluster.id')
if [[ ${HUB_CLUSTER_ID} != ${CONF_CLUSTER_ID} ]]; then
  echo "Env var HUB_CLUSTER_ID (${HUB_CLUSTER_ID}) must match alluxio.hub.cluster.id (${CONF_CLUSTER_ID})"
  exit 1
fi
# logstash requires index to be in lowercase
export HUB_CLUSTER_ID=$(echo "${HUB_CLUSTER_ID}" | tr '[:upper:]' '[:lower:]')
set +a

# copy logstash configuration file to logstash installation directory
cp -f ${SCRIPTPATH}/logstash.conf ${LOGSTASH_DIR}/logstash.conf
if [[ "${USE_DEV}" == "true" ]]; then
  perl -pi -e "s/# ssl_certificate_verification/ssl_certificate_verification/" ${LOGSTASH_DIR}/logstash.conf
fi

cd ${LOGSTASH_DIR}

# test configuration before starting
bin/logstash --config.test_and_exit -f logstash.conf
# start logstash
bin/logstash -f logstash.conf --config.reload.automatic
