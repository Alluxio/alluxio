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

DIST="/vagrant/shared/${MESOS_DIST}"

if [[ ! -f "${DIST}" ]]; then
  version=$(echo "${DIST}" | cut -d'-' -f2)
  wget -q "https://github.com/apache/mesos/releases/download/v${version}/${MESOS_DIST}" -P /vagrant/shared
fi

tar xzf "${DIST}" -C /mesos --strip-components 1
