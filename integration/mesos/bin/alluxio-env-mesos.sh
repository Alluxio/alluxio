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

export ALLUXIO_CONF_DIR="${MESOS_SANDBOX}/conf"
export ALLUXIO_LOGS_DIR="${MESOS_SANDBOX}/logs"

echo "${ALLUXIO_MESOS_SITE_PROPERTIES_CONTENT}" > ${ALLUXIO_CONF_DIR}/alluxio-site.properties

if [[ -z "${ALLUXIO_RAM_FOLDER}" ]]; then
    if [[ $(uname -s) == Darwin ]]; then
        # Assuming Mac OS X
        ALLUXIO_RAM_FOLDER="/Volumes/ramdisk"
    else
        # Assuming Linux
        ALLUXIO_RAM_FOLDER="/mnt/ramdisk"
    fi
fi
