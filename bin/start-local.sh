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

sleep 1

$bin/mount-ramfs.sh SudoMount

$bin/start-master.sh

sleep 2

echo "Starting worker @ `hostname`"
(java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="WORKER_LOGGER" -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties $TACHYON_JAVA_OPTS tachyon.Worker `hostname`) &
