#!/bin/bash

Usage="Usage: thrift-gen.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

rm -rf $bin/../tachyon/src/main/java/tachyon/thrift

thrift --gen java -out $bin/../tachyon/src/main/java/. $bin/../tachyon/src/thrift/tachyon.thrift
