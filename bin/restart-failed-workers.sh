#!/usr/bin/env bash

Usage="Usage: restart-failed-workers.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

WAIT_FOR_SSH=false $bin/slaves.sh $bin/restart-failed-worker.sh
