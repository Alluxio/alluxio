#!/usr/bin/env bash

Usage="Usage: restart-failed-tachyon-workers.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

$bin/slaves.sh $bin/restart-failed-tachyon-worker.sh