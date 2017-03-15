#!/usr/bin/env bash

set -e

cd /mesos
./bootstrap
mkdir -p build
cd build
../configure
make -j4
sudo make install
