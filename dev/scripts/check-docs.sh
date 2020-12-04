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
# This script validates headers and relative links in documentation markdown files
# and verifies markdown is buildable by jekyll

set -eux

SCRIPT_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

GOPATH="${SCRIPT_DIR}" go run "${SCRIPT_DIR}/src/alluxio.org/check-docs/main.go"

cd "${SCRIPT_DIR}"/../../docs && jekyll build --trace

