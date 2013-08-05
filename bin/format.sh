#!/bin/bash

Usage="Usage: format.sh"

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

echo "Formatting Tachyon @ $MASTER_ADDRESS"
# echo "$JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true $TACHYON_JAVA_OPTS tachyon.Format"
$JAVA_HOME/bin/java -cp $TACHYON_JAR -Dtachyon.home=$TACHYON_HOME -Dtachyon.is.system=true $TACHYON_JAVA_OPTS tachyon.Format
