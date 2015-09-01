#!/usr/bin/env bash

set -e

cd /mesos
./bin/mesos-master --work-dir=/mesos/workdir/master --port=1234