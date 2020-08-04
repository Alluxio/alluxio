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

. $(dirname "$0")/alluxio-common.sh

USAGE="Usage: alluxio-mount.sh [Mount|SudoMount|Umount|SudoUmount] [MACHINE]
\nIf omitted, MACHINE is default to be 'local'. MACHINE is one of:\n
  local\t\t\tMount local machine\n
  workers\t\tMount all the workers on worker nodes"

function init_env() {
  local libexec_dir=${ALLUXIO_LIBEXEC_DIR:-"${BIN}"/../libexec}
  . ${libexec_dir}/alluxio-config.sh
  RAMDISK_SIZE=$(${BIN}/alluxio getConf --unit B alluxio.worker.ramdisk.size)
  TIER_ALIAS=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
  get_ramdisk_array
}

function check_space_linux() {
  local total_mem=$(($(cat /proc/meminfo | awk 'NR==1{print $2}') * 1024))
  if [[ ${total_mem} -lt ${RAMDISK_SIZE} ]]; then
    echo "ERROR: Memory(${total_mem}) is less than requested ramdisk size(${RAMDISK_SIZE}). Please
    reduce alluxio.worker.ramdisk.size in alluxio-site.properties" >&2
    exit 1
  fi
}

function mount_ramfs_linux() {
  TIER_PATH=${1}
  echo "Formatting RamFS: ${TIER_PATH} (${RAMDISK_SIZE})"
  if [[ ${USE_SUDO} == true ]]; then
    sudo mkdir -p ${TIER_PATH}
  else
    mkdir -p ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mkdir ${TIER_PATH} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo mount -t ramfs -o size=${RAMDISK_SIZE} ramfs ${TIER_PATH}
  else
    mount -t ramfs -o size=${RAMDISK_SIZE} ramfs ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mount RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo chmod a+w ${TIER_PATH}
  else
    chmod a+w ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: chmod RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi
}

function umount_ramfs_linux() {
  TIER_PATH=${1}
  if mount | grep -E "(^|[[:space:]])${TIER_PATH}($|[[:space:]])" > /dev/null; then
    echo "Unmounting ${TIER_PATH}"
    if [[ ${USE_SUDO} == true ]]; then
      sudo umount -l -f ${TIER_PATH}
    else
      umount -l -f ${TIER_PATH}
    fi
    if [[ $? -ne 0 ]]; then
      echo "ERROR: umount RamFS ${TIER_PATH} failed" >&2
      exit 1
    fi
  fi
}

function check_space_freebsd() {
  local total_mem=$(sysctl -n hw.usermem)
  if [[ ${total_mem} -lt ${RAMDISK_SIZE} ]]; then
    echo "ERROR: Memory(${total_mem}) is less than requested ramdisk size(${RAMDISK_SIZE}). Please
    reduce alluxio.worker.ramdisk.size in alluxio-site.properties" >&2
    exit 1
  fi
}

function mount_ramfs_freebsd() {
  TIER_PATH=${1}
  echo "Formatting RamFS: ${TIER_PATH} (${RAMDISK_SIZE})"
  if [[ ${USE_SUDO} == true ]]; then
    sudo mkdir -p ${TIER_PATH}
  else
    mkdir -p ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mkdir ${TIER_PATH} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo mount -t tmpfs -o size=${RAMDISK_SIZE} tmpfs ${TIER_PATH}
  else
    mount -t tmpfs -o size=${RAMDISK_SIZE} tmpfs ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mount RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi

  if [[ ${USE_SUDO} == true ]]; then
    sudo chmod a+w ${TIER_PATH}
  else
    chmod a+w ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: chmod RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi
}

function umount_ramfs_freebsd() {
  TIER_PATH=${1}
  if mount | grep -E "(^|[[:space:]])${TIER_PATH}($|[[:space:]])" > /dev/null; then
    echo "Unmounting ${TIER_PATH}"
    if [[ ${USE_SUDO} == true ]]; then
      sudo umount -f ${TIER_PATH}
    else
      umount -f ${TIER_PATH}
    fi
    if [[ $? -ne 0 ]]; then
      echo "ERROR: umount RamFS ${TIER_PATH} failed" >&2
      exit 1
    fi
  fi
}

function mount_ramfs_mac() {
  TIER_PATH=${1}
  # Convert the memory size to number of sectors. Each sector is 512 Byte.
  local num_sectors=$(${BIN}/alluxio runClass alluxio.util.HFSUtils ${RAMDISK_SIZE} 512)

  # Format the RAM FS
  echo "Formatting RamFS: ${TIER_PATH} ${num_sectors} sectors (${RAMDISK_SIZE})."
  # Remove the "/Volumes/" part so we can get the name of the volume.
  diskutil erasevolume HFS+ ${TIER_PATH/#\/Volumes\//} $(hdiutil attach -nomount ram://${num_sectors})
}

function umount_ramfs_mac() {
  TIER_PATH=${1}
  local device=$(df -l | grep -E "(^|[[:space:]])${TIER_PATH}($|[[:space:]])" | cut -d " " -f 1)
  if [[ -n "${device}" ]]; then
    echo "Unmounting ramfs at ${TIER_PATH}"
    hdiutil detach -force ${device}
  else
    echo "Ramfs is not currently mounted at ${TIER_PATH}"
  fi
}

function mount_ramfs_local_all() {
  for RAMDISKPATH in "${RAMDISKARRAY[@]}"
  do
    mount_ramfs_local $RAMDISKPATH
  done
}

function mount_ramfs_local() {
  if [[ $(uname -a) == Darwin* ]]; then
    # Assuming Mac OS X
    umount_ramfs_mac $1
    mount_ramfs_mac $1
  elif [[ $(uname -a) == FreeBSD* ]]; then
    # Assuming FreeBSD
    check_space_freebsd
    umount_ramfs_freebsd $1
    mount_ramfs_freebsd $1
  else
    # Assuming Linux
    check_space_linux
    umount_ramfs_linux $1
    mount_ramfs_linux $1
  fi
}

function umount_ramfs_local_all() {
  for RAMDISKPATH in "${RAMDISKARRAY[@]}"
  do 
    umount_ramfs_local $RAMDISKPATH
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
