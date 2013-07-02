#!/usr/bin/env bash

# Start all Tachyon workers.
# Starts the master on this node.
# Starts a worker on each node specified in conf/slaves

Usage="Usage: mount.sh [Mount|SudoMount]"

if [ "$#" -ne 1 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [[ "$1" == "Mount" ]] ; then
  $bin/slaves.sh $bin/mount-ramfs.sh $1
elif [[ "$1" == "SudoMount" ]]; then
  $bin/slaves.sh $bin/mount-ramfs.sh $1
else
  echo $Usage
  exit 1
fi
