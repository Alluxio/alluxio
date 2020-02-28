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
    pushd "${SCRIPT_DIR}/fpm"
    docker build -t fpm:latest .
    popd
}

main() {
    local deb_pkg="${1}"
    if ! docker image inspect deb-builder:latest || true; then
        build_docker_image
    fi

    local deb_package="$(basename "$(realpath "${deb_pkg}")")"
    local deb_dir="$(dirname "$(realpath "${deb_pkg}")")"
    docker run -i \
    -v "$(realpath "${deb_dir}")":/data \
    fpm:latest fpm --input-type deb --output-type rpm -p /data "/data/${deb_package}"
}

main "$@"
