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

rm -rf $TACHYON_HOME/logs
mkdir -p $TACHYON_HOME/logs
mkdir -p $TACHYON_HOME/data

$bin/stop.sh

if [ -z $TACHYON_RAM_FOLDER ] ; then
  echo "TACHYON_RAM_FOLDER was not set. Using the default one: /mnt/ramfs"
  TACHYON_RAM_FOLDER=/mnt/ramfs
fi

F=$TACHYON_RAM_FOLDER

echo "Formatting RamFS: $F"
sudo mkdir -p $F; sudo mount -t ramfs -o size=15g ramfs $F ; sudo chmod a+w $F ;

$bin/start-master.sh

sleep 1

echo "Starting worker @ `hostname`"
# echo "Starting worker: (java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true $TACHYON_JAVA_OPTS tachyon.Worker `hostname`) &> $TACHYON_HOME/logs/worker.log &"
(java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true $TACHYON_JAVA_OPTS tachyon.Worker `hostname`) &> $TACHYON_HOME/logs/worker.log &
