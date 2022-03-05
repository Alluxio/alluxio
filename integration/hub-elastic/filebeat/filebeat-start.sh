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
# - Filebeat OSS 7.12.1 is being used
# - Clean installation
# - Downloads available here: https://www.elastic.co/downloads/past-releases/filebeat-oss-7-12-1 

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
Usage: filebeat-start.sh [-s] /path/to/filebeat

    /path/to/filebeat    Path to filebeat directory

EOF
)
  
if [[ "$#" -ne 1 ]]; then
  echo "Arguments /path/to/filebeat' must be provided."
  echo "${HELP}"
  exit 1
fi

FILEBEAT_DIR="${1}"

if [[ ! -d ${FILEBEAT_DIR} ]]; then 
  echo "Directory ${FILEBEAT_DIR} DOES NOT exists."
  exit 1
fi

# copy filebeat configuration files to filebeat installation directory
cp -f ${SCRIPTPATH}/filebeat.yml ${FILEBEAT_DIR}/filebeat.yml
sed -i '' -e "s#<PATH/TO/ALLUXIO/LOGS>#$(get_conf 'alluxio.logs.dir')#" ${FILEBEAT_DIR}/filebeat.yml
sed -i '' -e "s/<LOGSTASH_HOST>/$(get_conf 'alluxio.hub.manager.rpc.hostname')/" ${FILEBEAT_DIR}/filebeat.yml

cd ${FILEBEAT_DIR}

# test all settings before starting
./filebeat test config
./filebeat test modules
./filebeat test output

# start filebeat
./filebeat run -e
