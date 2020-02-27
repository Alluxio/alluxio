#!/usr/bin/env bash
set -eux

USAGE="build.sh <alluxio tarball> [<output file>]
"

build_docker_image() {
    docker build -t deb-builder:latest .
}

main() {
    local alluxio_tarball="${1}"
    if ! docker image inspect deb-builder:latest || true; then
        build_docker_image
    fi


    local tarball="$(basename "$(realpath ${alluxio_tarball})")"
    local version="${tarball#alluxio-}"
    version="${version%%-bin.tar.gz}"

    local control="
Package: alluxio
Source: alluxio-src
Version: ${version}
Section: base
Priority: optional
Architecture: all
Depends: openjdk-8-jre, openjdk-8-jdk
Maintainer: Zachary Blanco <zac@alluxio.com>
Description: Alluxio debian distribution
 This is a debian-packaged version of Alluxio (https://alluxio.io)

"

    local package_name="alluxio_${version}-0"

    cat ${alluxio_tarball} | \
    docker run --rm -i deb-builder:latest "${package_name}" "${control}" \
    > "${package_name}.deb"
}

main "$@"