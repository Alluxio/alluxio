#!/usr/bin/env bash

sudo service glusterd start
sudo mkdir -p /gfs_vol
sudo chown -R $(whoami)
sudo mkdir -p /vol
sudo chown -R $(whoami) /vol
