#!/usr/bin/env bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

# Utility functions for alluxio scripts 

# Generates an array of ramdisk paths in global variable RAMDISKARRAY 
function get_ramdisk_array() {
  local tier_path=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)
  local medium_type=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.mediumtype)
  local patharray
  local mediumtypearray
  local oldifs=$IFS
  IFS=','
  read -ra patharray <<< "$tier_path"
  read -ra mediumtypearray <<< "$medium_type"
  
  RAMDISKARRAY=()
  for i in "${!patharray[@]}"; do 
    if [ "${mediumtypearray[$i]}" = "MEM" ]; then
      RAMDISKARRAY+=(${patharray[$i]})
    fi
  done
  IFS=$oldifs
}
