#!/usr/bin/env bash

HADOOP_VERSION="1.0.4"

TARBALL_NAME=${TARBALL_URL##*/}

if [[ ! -f "/vagrant/shared/$TARBALL_NAME" ]]; then
  sudo yum install -y -q wget
  wget -q ${TARBALL_URL} -P /vagrant/shared
fi

tar xzf /vagrant/shared/${TARBALL_NAME} -C /hadoop --strip-components 1
