#!/usr/bin/env bash

main() {
    local deb_package="$(basename "$(realpath alluxio*.deb)")"
    docker run -i \
    -v "$(realpath .)":/data \
    fpm:latest fpm --input-type deb --output-type rpm -p /data "/data/${deb_package}"
}

main "$@"