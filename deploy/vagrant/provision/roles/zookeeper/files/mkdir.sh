#!/usr/bin/env bash

if [[ ! -d /zookeeper ]]; then
  sudo mkdir /zookeeper
  sudo chown -R $(whoami) /zookeeper
fi
