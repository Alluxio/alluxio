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

BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

echo "The alluxio-start.sh script is deprecated. Use the \"bin/alluxio process start\" command to start processes."

# while the leading argument starts with a -, continue to parse flags
# this will skip the mount related arguments that are no longer relevant and would cause an error to the new start command
startArgs=()
for arg in "$@"; do
  startArgs+=("${arg}")
  if [[ "${arg}" != -* ]]; then
    break
  fi
done

"${BIN}"/alluxio process start "${startArgs[@]}"
