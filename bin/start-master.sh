#!/bin/bash

Usage="Usage: start-master.sh"

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

MASTER_ADDRESS=$TACHYON_MASTER_ADDRESS
if [ -z $TACHYON_MASTER_ADDRESS ] ; then
  MASTER_ADDRESS=localhost
fi

echo "Starting master @ $MASTER_ADDRESS"
#echo "$JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="MASTER_LOGGER" -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties $TACHYON_JAVA_OPTS tachyon.Master) &"
($JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.logger.type="MASTER_LOGGER" -Dlog4j.configuration=file:$TACHYON_HOME/conf/log4j.properties $TACHYON_JAVA_OPTS tachyon.Master) &
