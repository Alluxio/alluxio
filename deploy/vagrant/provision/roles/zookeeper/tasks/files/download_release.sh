#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST="/vagrant/shared/zookeeper-${ZOOKEEPER_VERSION}.tar.gz"

if [[ ! -f "${DIST}" ]]; then
  wget -q "http://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz" -P /vagrant/shared
fi

tar xzf "${DIST}" -C /zookeeper --strip-components 1
