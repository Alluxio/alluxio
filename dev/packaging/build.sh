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
set -eux

SCRIPT_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

build_docker_image() {
    pushd "${SCRIPT_DIR}/debian"
    docker build -t deb-builder:latest .
    popd
}

build_debian() {
    local alluxio_tarball="${1}"
    if ! docker image inspect deb-builder:latest || true; then
        build_docker_image
    fi

    local tarball_name="$(basename "$(realpath ${alluxio_tarball})")"
    local version="${tarball_name#alluxio-}"
    version="${version%%-bin*.tar.gz}"

    local package_name="alluxio_${version}-0"

    cat ${alluxio_tarball} | \
    docker run --rm -i \
    -v "${SCRIPT_DIR}/debian":/data \
    deb-builder:latest "${package_name}" "${version}" \
    > "${package_name}.deb"
}

main() {
    build_debian "${1}"
}
main "$@"
