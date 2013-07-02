#!/usr/bin/env bash

Usage="Usage: mount-ramfs.sh [Mount|SudoMount]"

if [ "$#" -ne 1 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [[ "$1" == "Mount" ]] ; then
  if [[ `uname -a` == Darwin* ]]; then
    # Assuming Mac OS X
    $bin/mount-ramfs-mac.sh
  else
    # Assuming Linux
    $bin/mount-ramfs-linux.sh
  fi
elif [[ "$1" == "SudoMount" ]]; then
  if [[ `uname -a` == Darwin* ]]; then
    # Assuming Mac OS X
    $bin/mount-ramfs-mac.sh
  else
    # Assuming Linux
    sudo $bin/mount-ramfs-linux.sh
  fi
else
  echo $Usage
  exit 1
fi