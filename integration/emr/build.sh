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

ALLUXIO_DOWNLOAD_URL=${1}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mkdir -p ${DIR}/.generated
cp ${DIR}/alluxio-emr.* ${DIR}/.generated

# replace ALLUXIO_DOWNLOAD_URL in emr bootstrap script (alluxio-emr.sh)
if [[ -n ${ALLUXIO_DOWNLOAD_URL} ]]; then
  perl -p -e "s|^readonly ALLUXIO_DOWNLOAD_URL.*\$|readonly ALLUXIO_DOWNLOAD_URL=\"${ALLUXIO_DOWNLOAD_URL}\"|" ${DIR}/alluxio-emr.sh > ${DIR}/.generated/alluxio-emr.sh
fi
