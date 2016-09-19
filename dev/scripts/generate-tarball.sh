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
# This script generates a tarball of the current Alluxio commit. It does the following:
# 1. Copy everything except logs/ and dev/ to a work directory
# 2. Clean out unrevisioned files
# 3. Compile
# 4. Use bin/alluxio version to determine the right directory name, e.g. alluxio-1.2.0
# 5. Tar everything up and put it in dev/scripts/tarballs
#

set -e

THIS=$(cd "$( dirname "$0" )"; pwd)
cd ${THIS}
TARBALL_DIR="tarballs"
WORK_DIR="workdir"

mkdir -p ${TARBALL_DIR}
mkdir -p ${WORK_DIR}
HOME="${THIS}/../.."

REPO_COPY=${WORK_DIR}/alluxio
rsync -aq --exclude='logs' --exclude='dev' ${HOME} ${REPO_COPY}

pushd ${REPO_COPY} > /dev/null
git clean -fdx
BUILD_LOG="${HOME}/logs/build.log"
echo "Running build and logging to ${BUILD_LOG}"
mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos ${BUILD_OPT} | tee ${BUILD_LOG} 2>&1
VERSION=$(${HOME}/bin/alluxio version)
PREFIX=alluxio-${VERSION}
cd ..
rm -rf ${PREFIX}
mv alluxio ${PREFIX}
popd > /dev/null

TARGET=${TARBALL_DIR}/${PREFIX}.tar.gz
gtar -czf ${TARGET} ${WORK_DIR}/${PREFIX} --exclude-vcs

echo "Generated tarball at ${THIS}/${TARGET}"
