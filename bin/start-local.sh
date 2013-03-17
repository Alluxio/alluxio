#!/usr/bin/env bash

# Start all Tachyon workers.
# Starts the master on this node.
# Starts a worker on each node specified in conf/slaves

Usage="Usage: start-local.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [ -e $TACHYON_HOME/conf/tachyon-env.sh ] ; then
  . $TACHYON_HOME/conf/tachyon-env.sh
fi

mkdir -p $TACHYON_HOME/logs
mkdir -p $TACHYON_HOME/data

$bin/stop.sh

if [[ `uname -a` == Darwin* ]]; then
  # Assuming Mac OS X
  echo "Mounting ramfs on Mac OS X..."
  $bin/mount-ramfs-mac.sh
else
  # Assuming Linux
  echo "Mounting ramfs on Linux..."
  sudo $bin/mount-ramfs-linux.sh
fi

$bin/start-master.sh

sleep 1

echo "Starting worker @ `hostname`"
(java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true $TACHYON_JAVA_OPTS tachyon.Worker `hostname`) &
