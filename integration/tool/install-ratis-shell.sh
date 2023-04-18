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

#
# This script is run from inside the Docker container
#
set -ex

TOOL_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)
# ratis-shell version
VERSION=$1
if [ -z "$VERSION" ]; then
  VERSION=2.4.1
fi

wget -P "$TOOL_DIR" "https://dlcdn.apache.org/ratis/$VERSION/apache-ratis-$VERSION-bin.tar.gz"
mkdir ratis-shell
tar -zxvf apache-ratis-$VERSION-bin.tar.gz -C $TOOL_DIR/ratis-shell --strip-component 1
chmod 755 ratis-shell/bin/ratis
rm apache-ratis-$VERSION-bin.tar.gz
