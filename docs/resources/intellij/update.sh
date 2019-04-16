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
UNPACK_DIR="${CALLING_DIR}/unpacked"
XML_LICENSE="$(cat ${CALLING_DIR}/LICENSE.txt)"

if [[ $# -ne 1 ]]; then
    echo "Please call with the jar to unpack, e.g. ./update AlluxioCodeStyle.jar" 
    exit 1
fi

if [[ ! -d "${UNPACK_DIR}" ]]; then
  mkdir "${UNPACK_DIR}"
fi

cd "${UNPACK_DIR}"

jar -xvf "${CALLING_DIR}/$1"

# Add license info
for x in "${UNPACK_DIR}/codestyles/*.xml"; do 
    content="$(cat ${x})" && echo -en "${XML_LICENSE}\n\n""${content}" > ${x}
done

for x in "${UNPACK_DIR}/options/*.xml"; do 
    content="$(cat ${x})" && echo -en "${XML_LICENSE}\n\n""${content}" > ${x}
done

# Update files
cp -rf "${UNPACK_DIR}/codestyles" "${CALLING_DIR}"
cp -rf "${UNPACK_DIR}/options" "${CALLING_DIR}"
cd "${CALLING_DIR}"
rm -rf "${UNPACK_DIR}"
echo "Local files updated. Please run git commit and git push to push these updates to your Git repository."
