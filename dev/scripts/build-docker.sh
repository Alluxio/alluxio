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

readonly SCRIPT_DIR=$(cd "$( dirname "$0" )"; pwd)
readonly DOCKER_DIR="${SCRIPT_DIR}/../../integration/docker"
readonly GENERATE_TARBALLS_SCRIPT="${SCRIPT_DIR}/generate-tarballs"

# Builds a docker image from the specified tarball.
function build_docker_image {
  local tarball=$1
  local tmp_dir="$(mktemp -d)"
  cp -r "${DOCKER_DIR}" "${tmp_dir}"
  cp "${tarball}" "${tmp_dir}/docker"
  cd "${tmp_dir}/docker"
  # example tarball: /path/to/workdir/alluxio-1.4.0-SNAPSHOT.tar.gz
  # docker image tags must be lowercase
  local tarball_basename=$(basename ${tarball})
  local tag=$(echo ${tarball_basename%.tar.gz} | tr '[:upper:]' '[:lower:]')
  echo "Building ${tag} image..."
  docker build -t "${tag}" --build-arg "ALLUXIO_TARBALL=${tarball_basename}" .
  rm -rf "${tmp_dir}"
}

function main {
  cd "${SCRIPT_DIR}"
  local tmp_dir="$(mktemp -d)"
  "${GENERATE_TARBALLS_SCRIPT}" single -target "${tmp_dir}/alluxio-\${VERSION}.tar.gz"
  local tarball="${tmp_dir}/$(ls -tr ${tmp_dir} | tail -1)"
  build_docker_image "${tarball}"
  rm -rf ${tmp_dir}
}

main "$@"
