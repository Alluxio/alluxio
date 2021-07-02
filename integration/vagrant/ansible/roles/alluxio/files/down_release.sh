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


mkdir -p /vagrant/shared

DIST=/vagrant/shared/${ALLUXIO_DIST}

if [[ ! -f ${DIST} ]]; then
  version=$(echo ${DIST} | cut -d'-' -f2)
  sudo yum install -y -q wget
  wget -q http://downloads.alluxio.io/downloads/files/${version}/${ALLUXIO_DIST} -P /vagrant/shared
  if [[ $? -ne 0 ]]; then
    echo "Failed to download alluxio distribution $ALLUXIO_DIST. Please " \
        "make sure your alluxio and hadoop versions are valid"
    exit 1
  fi
fi
