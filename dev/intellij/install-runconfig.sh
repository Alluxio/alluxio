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

#For more details on how to use this script, go
#[to the docs](https://docs.alluxio.io/os/user/stable/en/contributor/Contributor-Tools.html).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SRC_DIR="$SCRIPT_DIR/runConfigurations"
DEST_DIR="$SCRIPT_DIR/../../.idea/runConfigurations/"
mkdir -p "$DEST_DIR"
ls -1 "$SRC_DIR" | xargs -n1 -I FILE cp "$SRC_DIR/FILE" "$DEST_DIR"
