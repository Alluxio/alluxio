#!/usr/bin/env bash

Usage="Usage: run-all-tests.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

declare -a opArr=(WRITE_CACHE WRITE_CACHE_THROUGH WRITE_THROUGH)

for op in ${opArr[@]}
do
  echo $bin/run-tests.sh Basic $op
  $bin/run-tests.sh Basic $op
  echo $bin/run-tests.sh BasicRawTable $op
  $bin/run-tests.sh BasicRawTable $op
done