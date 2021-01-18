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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SRC_DIR="$SCRIPT_DIR/runConfigurations"
DEST_DIR="$SCRIPT_DIR/../../.idea/runConfigurations/"
echo `realpath $DEST_DIR`
mkdir -p "$DEST_DIR"
#shellcheck disable=SC2010
ls -1 "$SRC_DIR" | xargs -n1 -I FILE cp "$SRC_DIR/FILE" "$DEST_DIR"