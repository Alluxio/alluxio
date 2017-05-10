#!/usr/bin/env bash

if [ ! -d /hibench ]; then
  sudo mkdir /hibench
  sudo chown -R `whoami` /hibench
fi
