#!/usr/bin/env bash

if [ ! -d /vagrant ]; then
 sudo mkdir /vagrant
 sudo chown -R $(whoami) /vagrant
fi
