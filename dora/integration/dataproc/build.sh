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

# Running this script will create edited versions of the files in the .generated directory
set -e

readonly ALLUXIO_DOWNLOAD_URL=${1}
readonly DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
readonly SCRIPT="alluxio-dataproc.sh"

mkdir -p ${DIR}/.generated
cp ${DIR}/${SCRIPT} ${DIR}/.generated

# replace ALLUXIO_DOWNLOAD_URL in dataproc init script
if [[ -n ${ALLUXIO_DOWNLOAD_URL} ]]; then
    if [[ -z $(grep "readonly ALLUXIO_DOWNLOAD_URL=" "${DIR}/${SCRIPT}") ]]; then
      echo "ERROR: unable to replace 'readonly ALLUXIO_DOWNLOAD_URL=' - pattern could not be found"
      exit 1
    fi
  perl -p -e "s|^readonly ALLUXIO_DOWNLOAD_URL.*\$|readonly ALLUXIO_DOWNLOAD_URL=\"${ALLUXIO_DOWNLOAD_URL}\"|" ${DIR}/${SCRIPT} > ${DIR}/.generated/${SCRIPT}
fi
