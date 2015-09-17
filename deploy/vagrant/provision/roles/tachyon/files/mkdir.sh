#!/usr/bin/env bash

if [ ! -d /tachyon ]; then
  sudo mkdir /tachyon
  sudo chown -R `whoami` /tachyon
fi
