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
# - Metricbeat OSS 7.12.1 is being used
# - Clean installation
# - Downloads available here: https://www.elastic.co/downloads/past-releases/metricbeat-oss-7-12-1

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
Usage: metricbeat-start.sh [-d] /path/to/metricbeat

    -d                     Set to disable verifying Elasticsearch ssl certificate. This is primarily used in
                           development/testing environments to connect to locally launched Elasticsearch clusters.
    /path/to/metricbeat    Path to metricbeat directory

EOF
)

if [[ "$#" -eq 0 ]]; then
  echo "Arguments /path/to/metricbeat' must be provided."
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

METRICBEAT_DIR="${1}"

if [[ ! -d ${METRICBEAT_DIR} ]]; then
  echo "Directory ${METRICBEAT_DIR} DOES NOT exists."
  exit 1
fi

if [[ ! -f ${SCRIPTPATH}/env ]]; then
  echo "env DOES NOT exists."
  exit 1
fi

# set environment variables used for metricbeat.yml
set -a # automatically export all variables
source ${SCRIPTPATH}/env
set +a

CONF_CLUSTER_ID=$(get_conf 'alluxio.hub.cluster.id')
if [[ ${HUB_CLUSTER_ID} != ${CONF_CLUSTER_ID} ]]; then
  echo "Env var HUB_CLUSTER_ID (${HUB_CLUSTER_ID}) must match alluxio.hub.cluster.id (${CONF_CLUSTER_ID})"
  exit 1
fi

# get master and worker hostnames w/ web port for prometheus metrics
MASTERS="$(get_conf 'alluxio.master.hostname'):$(get_conf 'alluxio.master.web.port')"
WORKERS=""
# get lines starting with 'Worker Name', remove one line ('Worker Name'), remove rest of line starting with ' ', trim all empty spaces
HOSTLIST=$(bin/alluxio fsadmin report capacity | sed -n '/Worker Name/,$p' | sed '1d' | cut -f1 -d' ' | sed 's/^ *//; s/ *$//; /^$/d')
for node in ${HOSTLIST[@]}; do
  WORKERS="${WORKERS}${node}:$(get_conf 'alluxio.worker.web.port'),"
done
WORKERS="$(echo ${WORKERS} | sed "s/,$//")"
ALL_HOSTNAMES="[${MASTERS},${WORKERS}]"

# copy metricbeat configuration files to metricbeat installation directory
cp -f ${SCRIPTPATH}/metricbeat.yml ${METRICBEAT_DIR}/metricbeat.yml
if [[ "${USE_DEV}" == "true" ]]; then
  sed -i '' -e "s/# ssl.verification_mode/ssl.verification_mode/" ${METRICBEAT_DIR}/metricbeat.yml
fi
# delete default modules.d
rm -rf ${METRICBEAT_DIR}/modules.d
# copy and enable prometheus module
cp -rf ${SCRIPTPATH}/modules.d ${METRICBEAT_DIR}
sed -i '' -e "s/<HOSTS_LIST>/${ALL_HOSTNAMES}/" ${METRICBEAT_DIR}/modules.d/prometheus.yml

cd ${METRICBEAT_DIR}

# test all settings before starting
./metricbeat test config
./metricbeat test modules
./metricbeat test output

# start metricbeat
./metricbeat run -e
