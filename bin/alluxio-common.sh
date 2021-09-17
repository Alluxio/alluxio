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

### Utility functions for alluxio scripts ###

# Fetches the specified property from Alluxio configuration
function get_alluxio_property() {
  local property_key="${1:-}"
  if [[ -z ${property_key} ]]; then
    echo "No property provided to get_alluxio_property()"
    exit 1
  fi

  local property=$(${BIN}/alluxio getConf ${property_key})
  if [[ ${?} -ne 0 ]]; then
    echo "Failed to fetch value for Alluxio property key: ${property_key}"
    exit 1
  fi

  echo "${property}"
}

# Generates an array of ramdisk paths in global variable RAMDISKARRAY
# - Only examines level0 of the tiered store for "MEM"-type paths
function get_ramdisk_array() {
  local tier_path=$(get_alluxio_property "alluxio.worker.tieredstore.level0.dirs.path")
  local medium_type=$(get_alluxio_property "alluxio.worker.tieredstore.level0.dirs.mediumtype")
  local patharray
  local mediumtypearray

  # Use "Internal Field Separator (IFS)" variable to split strings on a
  # delimeter and parse into an array
  # - https://stackoverflow.com/a/918931
  local oldifs=$IFS
  IFS=','
  read -ra patharray <<< "$tier_path"
  read -ra mediumtypearray <<< "$medium_type"

  # iterate over the array elements using indices
  # - https://stackoverflow.com/a/6723516
  RAMDISKARRAY=()
  for i in "${!patharray[@]}"; do 
    if [ "${mediumtypearray[$i]}" = "MEM" ]; then
      local dir=${patharray[$i]}
      if [[ -z "${dir}" ]]; then
        echo "Alluxio has a configured ramcache with an empty path"
        exit 1
      fi

      RAMDISKARRAY+=(${patharray[$i]})
    fi
  done
  IFS=$oldifs
}
