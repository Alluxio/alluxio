#!/bin/bash
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the “License”). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

CALLING_DIR="$(pwd)"

# Note, intentionally don't use absolute paths since otherwise that structure is saved in the jar file
jar -cf AlluxioCodeStyle.jar "codestyles/AlluxioStyle.xml" "options/code.style.schemes.xml"
