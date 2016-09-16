#!/usr/bin/env bash

set -e

THIS=$(cd "$( dirname "$0" )"; pwd)
TARBALL_DIR="${THIS}/tarballs"
WORK_DIR="workdir"

mkdir -p ${TARBALL_DIR}
mkdir -p ${WORK_DIR}
HOME="${THIS}/../.."

pushd ${HOME} > /dev/null
#mvn -T 4C clean install -Dmaven.javadoc.skip=true -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true -Pmesos ${BUILD_OPT}
popd > /dev/null

VERSION=$(${HOME}/bin/alluxio version)
PREFIX=alluxio-${VERSION}
DIRNAME=${WORK_DIR}/${PREFIX}

rm -rf ${DIRNAME}

rsync -aq --exclude='.git*' --exclude='log' --exclude='dev' ${HOME} ${DIRNAME}

gtar -czf ${TARBALL_DIR}/${PREFIX}.tar.gz ${DIRNAME}
