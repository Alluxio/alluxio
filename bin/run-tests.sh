#!/usr/bin/env bash

Usage="Usage: run-tests.sh <Basic/BasicRawTable>"

if [ "$#" -ne 1 ]; then
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

if [[ "$1" == "Basic" ]]; then
  java -cp $TACHYON_JAR tachyon.examples.BasicUserOperationTest $MASTER_ADDRESS /BasicFile
  exit 0
elif [[ "$1" == "BasicRawTable" ]]; then
  java -cp $TACHYON_JAR tachyon.examples.BasicRawTableTest $MASTER_ADDRESS /BasicRawTable
  exit 0
fi

echo $Usage
