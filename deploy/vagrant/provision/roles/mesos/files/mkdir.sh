#!/usr/bin/env bash

if [[ ! -d /mesos ]]; then
  sudo mkdir /mesos
  sudo chown -R $(whoami) /mesos
fi
