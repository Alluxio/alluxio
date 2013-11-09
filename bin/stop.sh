#!/usr/bin/env bash

usage="Usage: stop.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

$bin/tachyon killAll tachyon.Master
$bin/tachyon killAll tachyon.Worker

$bin/slaves.sh $bin/tachyon killAll tachyon.Worker