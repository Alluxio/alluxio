#!/usr/bin/env bash

set -e

cd /mesos
./bootstrap
mkdir build
cd build
../configure
make
make install
