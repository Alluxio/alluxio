#!/usr/bin/env bash

if [[ ! -d /hadoop ]]; then
 sudo mkdir /hadoop
 sudo chown -R $(whoami) /hadoop
fi