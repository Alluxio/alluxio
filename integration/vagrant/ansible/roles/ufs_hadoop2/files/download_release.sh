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


# get last field of url cut by '/'
TARBALL_NAME=${TARBALL_URL##*/}

if [[ ! -f "/vagrant/shared/$TARBALL_NAME" ]]; then
  # download hadoop
  sudo yum install -y -q wget
  wget -q "$TARBALL_URL" -P /vagrant/shared
fi

tar xzf "/vagrant/shared/$TARBALL_NAME" -C /hadoop --strip-components 1
