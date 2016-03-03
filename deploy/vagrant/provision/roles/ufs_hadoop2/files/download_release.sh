#!/usr/bin/env bash

# get last field of url cut by '/'
TARBALL_NAME=${TARBALL_URL##*/}

if [[ ! -f "/vagrant/shared/$TARBALL_NAME" ]]; then
 # download hadoop
 sudo yum install -y -q wget
 wget -q "$TARBALL_URL" -P /vagrant/shared
fi

tar xzf "/vagrant/shared/$TARBALL_NAME" -C /hadoop --strip-components 1
