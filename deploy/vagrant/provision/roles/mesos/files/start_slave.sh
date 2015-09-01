#!/usr/bin/env bash

set -e

cd /mesos
./bin/mesos-slave --work-dir=/mesos/workdir/slave --port=1235 --master=TachyonMaster:1234