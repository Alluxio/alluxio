#!/usr/bin/env bash
set -eux

main () {
    local pkg_name="${1}"
    local control="${2}"

    tee "alluxio.tar.gz" > /dev/null
    local pkg_dir="/${pkg_name}"
    mkdir -p "${pkg_dir}/DEBIAN"
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