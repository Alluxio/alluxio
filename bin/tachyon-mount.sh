#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Start all Tachyon workers.
# Starts the master on this node.
# Starts a worker on each node specified in conf/slaves

Usage="Usage: tachyon-mount.sh [Mount|SudoMount] [MACHINE]
\nIf ommited, MARCHINE is default to be 'local'. MARCHINE is one of:\n
  local\t\t\tMount local marchine\n
  workers\t\tMount all the workers on slaves"

function init_env() {
  bin=`cd "$( dirname "$1" )"; pwd`

  DEFAULT_LIBEXEC_DIR="$bin"/../libexec
  TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $TACHYON_LIBEXEC_DIR/tachyon-config.sh

  if [ -z TACHYON_WORKER_MEMORY_SIZE ] ; then
    TACHYON_WORKER_MEMORY_SIZE=128MB
  fi

  MEM_SIZE=$(echo "$TACHYON_WORKER_MEMORY_SIZE" | tr -s '[:upper:]' '[:lower:]')
}

function mount_ramfs_linux() {
  init_env $1

  if [ -z $TACHYON_RAM_FOLDER ] ; then
    TACHYON_RAM_FOLDER=/mnt/ramdisk
    echo "TACHYON_RAM_FOLDER was not set. Using the default one: $TACHYON_RAM_FOLDER"
  fi

  F=$TACHYON_RAM_FOLDER
  echo "Formatting RamFS: $F ($MEM_SIZE)"
  if mount | grep $F > /dev/null; then
    umount -f $F
  else
    mkdir -p $F
  fi

  mount -t ramfs -o size=$MEM_SIZE ramfs $F ; chmod a+w $F ;
}

#enable the regexp case match
shopt -s extglob
function mount_ramfs_mac() {
  init_env $0

  if [ -z $TACHYON_RAM_FOLDER ] ; then
    TACHYON_RAM_FOLDER=/Volumes/ramdisk
    echo "TACHYON_RAM_FOLDER was not set. Using the default one: $TACHYON_RAM_FOLDER"
  fi

  if [[ $TACHYON_RAM_FOLDER != "/Volumes/"* ]]; then
    echo "Invalid TACHYON_RAM_FOLDER: $TACHYON_RAM_FOLDER"
    echo "TACHYON_RAM_FOLDER must set to /Volumes/[name] on Mac OS X."
    exit 1
  fi

  # Remove the "/Volumes/" part so we can get the name of the volume.
  F=${TACHYON_RAM_FOLDER/#\/Volumes\//}

  # Convert the memory size to number of sectors. Each sector is 512 Byte.
  #SIZE is the decimal part of MEM_SIZE
  SIZE=${MEM_SIZE//[^0-9]/}
  case ${MEM_SIZE} in
    *g?(b))
      # Size was specified in gigabytes.
      NUM_SECTORS=$(($SIZE * 1024 * 2048))
      ;;
    *m?(b))
      # Size was specified in megabytes.
      NUM_SECTORS=$(($SIZE * 2048))
      ;;
    *k?(b))
      # Size was specified in kilobytes.
      NUM_SECTORS=$(($SIZE * 2))
      ;;
    +([0-9])?(b))
      # Size was specified in bytes.
      NUM_SECTORS=$((SIZE / 512))
      ;;
    *)
      echo "Please specify TACHYON_WORKER_MEMORY_SIZE in a correct form."
      exit 1
  esac

  echo "Formatting RamFS: $F $NUM_SECTORS sectors ($MEM_SIZE)."
  diskutil unmount force /Volumes/$F
  diskutil erasevolume HFS+ $F `hdiutil attach -nomount ram://$NUM_SECTORS`
}

function mount_local() {
  if [[ `uname -a` == Darwin* ]]; then
    # Assuming Mac OS X
    mount_ramfs_mac
  else
    # Assuming Linux
    if [[ "$1" == "SudoMount" ]]; then
      DECL_INIT=`declare -f init_env`
      DECL_MOUNT_LINUX=`declare -f mount_ramfs_linux`
      sudo bash -c "$DECL_INIT; $DECL_MOUNT_LINUX; mount_ramfs_linux $0"
    else
      mount_ramfs_linux $0
    fi
  fi
}

case "${1}" in
  Mount|SudoMount)
    case "${2}" in
      ""|local)
        mount_local $1
        ;;
      workers)
        $bin/tachyon-slaves.sh $bin/tachyon-mount.sh $1
        ;;
    esac
    ;;
  *)
    echo -e $Usage
    exit 1
esac