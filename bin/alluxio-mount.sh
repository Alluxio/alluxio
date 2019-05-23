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

# Starts the Alluxio master on this node.
# Starts an Alluxio worker on each node specified in conf/workers

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi
BIN=$(cd "$( dirname "$( readlink "$0" || echo "$0" )" )"; pwd)

USAGE="Usage: alluxio-mount.sh [Mount|SudoMount|Umount|SudoUmount] [MACHINE]
\nIf omitted, MACHINE is default to be 'local'. MACHINE is one of:\n
  local\t\t\tMount local machine\n
  workers\t\tMount all the workers on worker nodes"

function init_env() {
  local libexec_dir=${ALLUXIO_LIBEXEC_DIR:-"${BIN}"/../libexec}
  . ${libexec_dir}/alluxio-config.sh
  MEM_SIZE=$(${BIN}/alluxio getConf --unit B alluxio.worker.memory.size)
  TIER_ALIAS=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
  TIER_PATH=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)
  MEDIUM_TYPE=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.mediumtype)
  IFS=','
  read -ra PATHARRAY <<< "$TIER_PATH"
  read -ra MEDIUMTYPEARRAY <<< "$MEDIUM_TYPE"
  RAMDISKARRAY=()
  for i in "${!PATHARRAY[@]}"; do # access each element of array
    if [ "${MEDIUMTYPEARRAY[$i]}" = "MEM" ]; then
      RAMDISKARRAY+=(${PATHARRAY[$i]})
    fi
  done
  IFS=' '
}

function check_space_linux() {
  local total_mem=$(($(cat /proc/meminfo | awk 'NR==1{print $2}') * 1024))
  if [[ ${total_mem} -lt ${MEM_SIZE} ]]; then
    echo "ERROR: Memory(${total_mem}) is less than requested ramdisk size(${MEM_SIZE}). Please
    reduce alluxio.worker.memory.size in alluxio-site.properties" >&2
    exit 1
  fi
}

function mount_ramfs_linux() {
  echo "Formatting RamFS: ${1} (${MEM_SIZE})"
  if [[ ${USE_SUDO} == true ]]; then
    sudo mkdir -p ${1}
  else
    mkdir -p ${1}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mkdir ${1} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo mount -t ramfs -o size=${MEM_SIZE} ramfs ${1}
  else
    mount -t ramfs -o size=${MEM_SIZE} ramfs ${1}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mount RamFS ${1} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo chmod a+w ${1}
  else
    chmod a+w ${1}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: chmod RamFS ${1} failed" >&2
    exit 1
  fi
}

function umount_ramfs_linux() {
  if mount | grep ${1} > /dev/null; then
    echo "Unmounting ${1}"
    if [[ ${USE_SUDO} == true ]]; then
      sudo umount -l -f ${1}
    else
      umount -l -f ${1}
    fi
    if [[ $? -ne 0 ]]; then
      echo "ERROR: umount RamFS ${1} failed" >&2
      exit 1
    fi
  fi
}

function mount_ramfs_mac() {
  # Convert the memory size to number of sectors. Each sector is 512 Byte.
  local num_sectors=$(${BIN}/alluxio runClass alluxio.util.HFSUtils ${MEM_SIZE} 512)

  # Format the RAM FS
  echo "Formatting RamFS: ${1} ${num_sectors} sectors (${MEM_SIZE})."
  # Remove the "/Volumes/" part so we can get the name of the volume.
  diskutil erasevolume HFS+ ${1/#\/Volumes\//} $(hdiutil attach -nomount ram://${num_sectors})
}

function umount_ramfs_mac() {
  local device=$(df -l | grep $1 | cut -d " " -f 1)
  if [[ -n "${device}" ]]; then
    echo "Unmounting ramfs at ${1}"
    hdiutil detach -force ${device}
  else
    echo "Ramfs is not currently mounted at ${1}"
  fi
}

function mount_ramfs_local_all() {
for i in "${RAMDISKARRAY[@]}"
do 
  mount_ramfs_local $i
done
}

function mount_ramfs_local() {
  if [[ $(uname -a) == Darwin* ]]; then
    # Assuming Mac OS X
    umount_ramfs_mac $1
    mount_ramfs_mac $1
  else
    # Assuming Linux
    check_space_linux
    umount_ramfs_linux $1
    mount_ramfs_linux $1
  fi
}

function umount_ramfs_local_all() {
for i in "${RAMDISKARRAY[@]}"
do 
  umount_ramfs_local $i
done
}

function umount_ramfs_local() {
  if [[ $(uname -a) == Darwin* ]]; then
    umount_ramfs_mac $1
  else
    umount_ramfs_linux $1
  fi
}

function run_local() {
  init_env

  if [[ ${TIER_ALIAS} != "MEM" ]]; then
    # the top tier is not MEM, skip
    exit 1
  fi

  mount_type=$1
  case "$mount_type" in
    Mount)
      USE_SUDO=false
      mount_ramfs_local_all
      ;;
    SudoMount)
      USE_SUDO=true
      mount_ramfs_local_all
      ;;
    Umount)
      USE_SUDO=false
      umount_ramfs_local_all
      ;;
    SudoUmount)
      USE_SUDO=true
      umount_ramfs_local_all
      ;;
    *)
      echo -e ${USAGE} >&2
      exit 1
  esac
}

function main {
  case "$1" in
    Mount|SudoMount|Umount|SudoUmount)
      case "$2" in
        ""|local)
          run_local $1
          ;;
        workers)
          ${LAUNCHER} ${BIN}/alluxio-workers.sh ${BIN}/alluxio-mount.sh $1
          ;;
        *)
          echo -e ${USAGE} >&2
          exit 1
      esac
      ;;
    *)
      echo -e ${USAGE} >&2
      exit 1
  esac
}

main "$@"
