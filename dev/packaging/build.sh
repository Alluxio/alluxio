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
set -eu

SCRIPT_DIR=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: build.sh <alluxio tarball>

This command uses a path to an alluxio tarball as an input and creates a debian-
compatible package from it.

Arguments:

<alluxio tarball>           A path to an alluxio tarball.

"

print_usage() {
    echo "${USAGE}"
}

build_docker_image() {
    pushd "${SCRIPT_DIR}/debian" > /dev/null
    printf "Building docker container ... "
    # supress output
    docker build -t deb-builder:latest . > /dev/null
    printf "done\n"
    popd > /dev/null
}

build_debian() {
    local alluxio_tarball="${1}"
    if ! docker image inspect deb-builder:latest > /dev/null || true; then
        build_docker_image
    fi

    if [[ ! -f "${alluxio_tarball}" ]]; then
        print_usage
        exit
    fi

    local tarball_name="$(basename "${alluxio_tarball}")"
    local version="${tarball_name#alluxio-}"
    version="${version%%-bin*.tar.gz}"

    local package_name="alluxio_${version}-0"

    printf "Building debian package ... "
    cat ${alluxio_tarball} | \
    docker run --rm -i \
    -v "${SCRIPT_DIR}/debian":/data \
    deb-builder:latest "${package_name}" "${version}" \
    > "${package_name}.deb"
    printf "done\n"
}

main() {
    build_debian "${1}"
}
main "$@"
