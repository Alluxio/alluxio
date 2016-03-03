#!/usr/bin/env bash

if [ ! -d /spark ]; then
 sudo mkdir /spark
 sudo chown -R $(whoami) /spark
fi
