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

main () {
    local pkg_name="${1}"
    local version="${2}"

    tee "alluxio.tar.gz" > /dev/null
    local pkg_dir="/${pkg_name}"
    mkdir -p "${pkg_dir}/DEBIAN"
    local template="$(cat /data/DEBIAN/control)"
    local control="$(eval "echo \"${template}\"")"
    echo "${control}" > "${pkg_dir}/DEBIAN/control"
    mkdir -p "${pkg_dir}/opt"
    tar -xzf *.tar.gz -C "${pkg_dir}/opt" && rm *.tar.gz
    mv "${pkg_dir}"/opt/alluxio* "${pkg_dir}/opt/alluxio"

    mkdir -p ${pkg_dir}/etc/profile.d/ && \
        echo "export PATH=\"${PATH}:/opt/alluxio/bin\"" >> ${pkg_dir}/etc/profile.d/10-${pkg_name}.sh

    cd /
    dpkg-deb --build "${pkg_name}" > /dev/null

    cat *.deb
}

main "$@"
