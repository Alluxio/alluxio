#!/usr/bin/env bash

set -e

THIS=$(cd "$( dirname "$0" )"; pwd)
TARBALL_DIR="${THIS}/tarballs"
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

echo "Generated tarball at ${TARGET}"
