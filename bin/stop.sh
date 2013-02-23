#!/usr/bin/env bash

usage="Usage: stop.sh"

if [ "$#" -ne 0 ]; then
  echo $Usage
  exit 1
fi

bin=`cd "$( dirname "$0" )"; pwd`

$bin/killall.sh tachyon.Master
$bin/killall.sh tachyon.Worker

$bin/slaves.sh $bin/killall.sh tachyon.Worker