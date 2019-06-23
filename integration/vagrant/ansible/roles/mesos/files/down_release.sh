#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST="/vagrant/shared/${MESOS_DIST}"

if [[ ! -f "${DIST}" ]]; then
  version=$(echo "${DIST}" | cut -d'-' -f2)
  wget -q "https://github.com/apache/mesos/releases/download/v${version}/${MESOS_DIST}" -P /vagrant/shared
fi

tar xzf "${DIST}" -C /mesos --strip-components 1
