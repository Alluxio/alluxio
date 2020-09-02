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

DIST="/vagrant/shared/zookeeper-${ZOOKEEPER_VERSION}.tar.gz"

if [[ ! -f "${DIST}" ]]; then
  wget -q "http://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz" -P /vagrant/shared
fi

tar xzf "${DIST}" -C /zookeeper --strip-components 1
