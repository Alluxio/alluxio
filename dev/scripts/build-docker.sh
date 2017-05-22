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
# This script generates a Docker image for the repository from its current state.
#

set -e

this=$(cd "$( dirname "$0" )"; pwd)
cd "${this}"
docker_dir="${this}/../../integration/docker"

#"${this}/generate-tarball.sh" --skipFrameworks
tarball=$(ls -tr tarballs | tail -1)

tmpdir="$(mktemp -d)"
cp -r "${docker_dir}" "${tmpdir}"
cp "tarballs/${tarball}" "${tmpdir}/docker"

cd "${tmpdir}/docker"
# example tarball: alluxio-1.4.0-SNAPSHOT.tar.gz
# docker image tags must be lowercase
tag=$(echo "${tarball%.tar.gz}" | tr '[:upper:]' '[:lower:]')

echo "Building ${tag} image..."
docker build -t "${tag}" --build-arg "ALLUXIO_TARBALL=${tarball}" .
rm -rf "${tmpdir}"
