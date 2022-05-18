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
#

SCRIPT_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

(cd ${SCRIPT_DIR}/src/alluxio.org/ && GO111MODULE=on go build -o checkdocs "check-docs/main.go")

${SCRIPT_DIR}/src/alluxio.org/checkdocs $@

rm ${SCRIPT_DIR}/src/alluxio.org/checkdocs
