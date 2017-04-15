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
BIN=$(cd "$( dirname "$0" )"; pwd)

USAGE="Usage: alluxio-mount.sh [Mount|SudoMount] [MACHINE]
\nIf omitted, MACHINE is default to be 'local'. MACHINE is one of:\n
  local\t\t\tMount local machine\n
  workers\t\tMount all the workers on worker nodes"

function init_env() {
  local libexec_dir=${ALLUXIO_LIBEXEC_DIR:-"${BIN}"/../libexec}
  . ${libexec_dir}/alluxio-config.sh

  MEM_SIZE=$(${BIN}/alluxio getConf --unit B alluxio.worker.memory.size)
  TIER_ALIAS=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.alias)
  TIER_PATH=$(${BIN}/alluxio getConf alluxio.worker.tieredstore.level0.dirs.path)
}

# Mac OS X HFS+ provisioning
# Arguments:
#   Requested filesystem size in bytes
#   Sector size
# Returns:
#   Total sectors of HFS+ including estimated metadata zone size,
#   guaranteeing available space of created HFS+ is not less than request size.
# Notes:
#   Metadata zone size is estimated by using specifications noted at
#   http://dubeiko.com/development/FileSystems/HFSPLUS/tn1150.html#MetadataZone,
#   which is slightly larger than actually metadata zone size created by hdiuitl command.
function mac_hfs_provision_sectors() {
  local request_size=$1
  local sector_size=$2
  local alloc_size=0
  local n_sectors=0
  local n_100g=0
  local n_kb=0
  local alloc_bitmap_size=0
  local ext_overflow_file_size=0
  local journal_file_size=0
  local catalog_file_size=0
  local hot_files_size=0
  local quota_users_file_size=0
  local quota_groups_file_size=0
  local alloc_size=0

  n_sectors=$((request_size / sector_size))
  n_100g=$(printf "%.0f" $(echo "scale=4; ${request_size}/107374182400 + 0.5" | bc)) # round up
  n_gb=$(printf "%.0f" $(echo "scale=4; ${request_size}/1073741824 + 0.5" | bc)) # round up
  n_kb=$(printf "%.0f" $(echo "scale=4; ${request_size}/1024 + 0.5" | bc)) #round up

  # allocation bitmap file: one bit per sector
  alloc_bitmap_size=$((n_sectors / 8))

  # extends overflow file: 4MB, plus 4MB per 100GB
  ext_overflow_file_size=$((4194304 * n_100g))

  # journal file: 8MB, plus 8MB per 100GB
  journal_file_size=$((8388608 * n_100g))

  # catalog file: 10bytes per KB
  catalog_file_size=$((10 * n_kb))

  # hot files: 5bytes per KB
  hot_files_size=$((5 * n_kb))

  # quota users file and quota groups file
  quota_users_file_size=$(((n_gb * 256 + 1) * 64))
  quota_groups_file_size=$(((n_gb * 32 + 1) * 64))

  metadata_size=$((alloc_bitmap_size + ext_overflow_file_size + journal_file_size + \
    catalog_file_size + hot_files_size + quota_users_file_size + quota_groups_file_size))

  alloc_size=$((request_size + metadata_size))
  echo $((alloc_size / sector_size))
}

function mount_ramfs_linux() {
  local total_mem=$(($(cat /proc/meminfo | awk 'NR==1{print $2}') * 1024))
  if [[ ${total_mem} -lt ${MEM_SIZE} ]]; then
    echo "ERROR: Memory(${total_mem}) is less than requested ramdisk size(${MEM_SIZE}). Please
    reduce alluxio.worker.memory.size in alluxio-site.properties" >&2
    exit 1
  fi

  echo "Formatting RamFS: ${TIER_PATH} (${MEM_SIZE})"
  if mount | grep ${TIER_PATH} > /dev/null; then
    if [[ "$1" == "SudoMount" ]]; then
      sudo umount -l -f ${TIER_PATH}
    else
      umount -l -f ${TIER_PATH}
    fi
    if [[ $? -ne 0 ]]; then
      echo "ERROR: umount RamFS ${TIER_PATH} failed" >&2
      exit 1
    fi
  else
    if [[ "$1" == "SudoMount" ]]; then
      sudo mkdir -p ${TIER_PATH}
    else
      mkdir -p ${TIER_PATH}
    fi
    if [[ $? -ne 0 ]]; then
      echo "ERROR: mkdir ${TIER_PATH} failed" >&2
      exit 1
    fi
  fi

  if [[ "$1" == "SudoMount" ]]; then
    sudo mount -t ramfs -o size=${MEM_SIZE} ramfs ${TIER_PATH}
  else
    mount -t ramfs -o size=${MEM_SIZE} ramfs ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: mount RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi

  if [[ "$1" == "SudoMount" ]]; then
    sudo chmod a+w ${TIER_PATH}
  else
    chmod a+w ${TIER_PATH}
  fi
  if [[ $? -ne 0 ]]; then
    echo "ERROR: chmod RamFS ${TIER_PATH} failed" >&2
    exit 1
  fi
}

function mount_ramfs_mac() {
  # Convert the memory size to number of sectors. Each sector is 512 Byte.
  local num_sectors=$(mac_hfs_provision_sectors ${MEM_SIZE} 512)

  # Format the RAM FS
  # We may have a pre-existing RAM FS which we need to throw away
  echo "Formatting RamFS: ${TIER_PATH} ${num_sectors} sectors (${MEM_SIZE})."
  local device=$(df -l | grep ${TIER_PATH} | cut -d " " -f 1)
  if [[ -n "${device}" ]]; then
    hdiutil detach -force ${device}
  fi
  # Remove the "/Volumes/" part so we can get the name of the volume.
  diskutil erasevolume HFS+ ${TIER_PATH/#\/Volumes\//} $(hdiutil attach -nomount ram://${num_sectors})
}

function mount_ramfs_local() {
  init_env

  if [[ ${TIER_ALIAS} != "MEM" ]]; then
    # the top tier is not MEM, skip
    exit 1
  fi

  if [[ $(uname -a) == Darwin* ]]; then
    # Assuming Mac OS X
    mount_ramfs_mac
  else
    # Assuming Linux
    mount_ramfs_linux $1
  fi
}

function main {
  case "$1" in
    Mount|SudoMount)
      case "$2" in
        ""|local)
          mount_ramfs_local $1
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
