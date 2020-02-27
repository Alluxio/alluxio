#!/usr/bin/env bash
set -x


main() {
    if ! ls | grep -q ".tar.gz"; then
        cp ../../alluxio-*.tar.gz .
    fi
    local tarball="$(basename "$(realpath alluxio-*.tar.gz)")"
    local version="${tarball#alluxio-}"
    version="${version%%-bin.tar.gz}"
    DOCKER_BUILTKIT=1 docker build \
    --build-arg ALLUXIO_TARBALL="${tarball}" \
    --build-arg VERSION="${version}" \
    --build-arg PKG_REVISION=0 \
    .
}

main "$@"