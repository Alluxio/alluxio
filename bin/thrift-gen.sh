#!/bin/bash

Usage="Usage: thrift-gen.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

rm -rf $bin/../src/main/java/tachyon/thrift

thrift --gen java -out $bin/../src/main/java/. $bin/../src/thrift/tachyon.thrift