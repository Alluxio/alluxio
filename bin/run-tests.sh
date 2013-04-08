#!/usr/bin/env bash

Usage="Usage: run-tests.sh <Basic/BasicRawTable> <WRITE_CACHE/WRITE_CACHE_THROUGH/WRITE_THROUGH>"

if [ "$#" -ne 2 ]; then
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
  java -cp $bin/../conf/:$TACHYON_JAR tachyon.examples.BasicOperations $MASTER_ADDRESS:19998 /Basic_File_$2 $2
  exit 0
elif [[ "$1" == "BasicRawTable" ]]; then
  java -cp $bin/../conf/:$TACHYON_JAR tachyon.examples.BasicRawTableOperations $MASTER_ADDRESS:19998 /Basic_Raw_Table_$2 $2
  exit 0
fi

echo $Usage
