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

set -e

THIS=$(cd "$( dirname "$0" )"; pwd)
cd ${THIS}
TARBALL_DIR="tarballs"
WORK_DIR="workdir"

mkdir -p ${TARBALL_DIR}
mkdir -p ${WORK_DIR}
HOME="${THIS}/../.."

pushd ${HOME} > /dev/null
echo "Running build and logging to build.log"
mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos ${BUILD_OPT} > build.log 2>&1
popd > /dev/null

VERSION=$(${HOME}/bin/alluxio version)
PREFIX=alluxio-${VERSION}
DIRNAME=${WORK_DIR}/${PREFIX}
TARGET=${TARBALL_DIR}/${PREFIX}.tar.gz

rm -rf ${DIRNAME}

rsync -aq --exclude='.git*' --exclude='log' --exclude='dev' ${HOME} ${DIRNAME}

gtar -czf ${TARGET} ${DIRNAME}

echo "Generated tarball at ${THIS}/${TARGET}"
