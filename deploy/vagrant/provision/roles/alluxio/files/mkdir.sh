#!/usr/bin/env bash

if [ ! -d /alluxio ]; then
  sudo mkdir /alluxio
  sudo chown -R `whoami` /alluxio
fi
