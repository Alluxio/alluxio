#!/usr/bin/env bash

# Start all Alluxio workers.
# Starts the master on this node.
# Starts a worker on each node specified in conf/workers

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
  DEFAULT_LIBEXEC_DIR="${BIN}"/../libexec
  ALLUXIO_LIBEXEC_DIR=${ALLUXIO_LIBEXEC_DIR:-${DEFAULT_LIBEXEC_DIR}}
  . ${ALLUXIO_LIBEXEC_DIR}/alluxio-config.sh

  if [[ -z ${ALLUXIO_WORKER_MEMORY_SIZE} ]]; then
    echo "ALLUXIO_WORKER_MEMORY_SIZE was not set. Using the default one: 128MB"
    ALLUXIO_WORKER_MEMORY_SIZE="128MB"
  fi

  MEM_SIZE=$(echo "${ALLUXIO_WORKER_MEMORY_SIZE}" | tr -s '[:upper:]' '[:lower:]')
}

#enable the regexp case match
shopt -s extglob
function mem_size_to_bytes() {
  float_scale=2
  function float_eval() {
    local stat=0
    local result=0.0
    if [[ $# -gt 0 ]]; then
      result=$(echo "scale=${float_scale}; $*" | bc -q 2>/dev/null)
      stat=$?
      if [[ ${stat} -eq 0  &&  -z "${result}" ]]; then stat=1; fi
    fi
    echo $( printf "%.0f" ${result} )
    return $( printf "%.0f" ${stat} )
  }

  SIZE=${MEM_SIZE//[^0-9.]/}
  case ${MEM_SIZE} in
    *g?(b) )
      # Size was specified in gigabytes.
      BYTE_SIZE=$(float_eval "${SIZE} * 1024 * 1024 * 1024")
      ;;
    *m?(b))
      # Size was specified in megabytes.
      BYTE_SIZE=$(float_eval "${SIZE} * 1024 * 1024")
      ;;
    *k?(b))
      # Size was specified in kilobytes.
      BYTE_SIZE=$(float_eval "${SIZE} * 1024")
      ;;
    +([0-9])?(b))
      # Size was specified in bytes.
      BYTE_SIZE=${SIZE}
      ;;
    *)
      echo "Please specify ALLUXIO_WORKER_MEMORY_SIZE in a correct form."
      exit 1
  esac
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
    catalog_file_size + hot_file_size + quota_users_file_size + quota_groups_file_size))

  alloc_size=$((request_size + metadata_size))
  echo $((alloc_size / sector_size))
}

function mount_ramfs_linux() {
  init_env $1

  if [[ -z ${ALLUXIO_RAM_FOLDER} ]]; then
    ALLUXIO_RAM_FOLDER=/mnt/ramdisk
    echo "ALLUXIO_RAM_FOLDER was not set. Using the default one: ${ALLUXIO_RAM_FOLDER}"
  fi

  mem_size_to_bytes
  TOTAL_MEM=$(($(cat /proc/meminfo | awk 'NR==1{print $2}') * 1024))
  if [[ ${TOTAL_MEM} -lt ${BYTE_SIZE} ]]; then
    echo "ERROR: Memory(${TOTAL_MEM}) is less than requested ramdisk size(${BYTE_SIZE}). Please
    reduce ALLUXIO_WORKER_MEMORY_SIZE"
    exit 1
  fi

  F=${ALLUXIO_RAM_FOLDER}
  echo "Formatting RamFS: ${F} (${MEM_SIZE})"
  if mount | grep ${F} > /dev/null; then
    umount -f ${F}
    if [[ $? -ne 0 ]]; then
      echo "ERROR: umount RamFS ${F} failed"
      exit 1
    fi
  else
    mkdir -p ${F}
  fi

  mount -t ramfs -o size=${MEM_SIZE} ramfs ${F} ; chmod a+w ${F} ;
}

function mount_ramfs_mac() {
  init_env $0

  if [[ -z ${ALLUXIO_RAM_FOLDER} ]]; then
    ALLUXIO_RAM_FOLDER=/Volumes/ramdisk
    echo "ALLUXIO_RAM_FOLDER was not set. Using the default one: ${ALLUXIO_RAM_FOLDER}"
  fi

  if [[ ${ALLUXIO_RAM_FOLDER} != "/Volumes/"* ]]; then
    echo "Invalid ALLUXIO_RAM_FOLDER: ${ALLUXIO_RAM_FOLDER}"
    echo "ALLUXIO_RAM_FOLDER must set to /Volumes/[name] on Mac OS X."
    exit 1
  fi

  # Remove the "/Volumes/" part so we can get the name of the volume.
  F=${ALLUXIO_RAM_FOLDER/#\/Volumes\//}

  # Convert the memory size to number of sectors. Each sector is 512 Byte.
  mem_size_to_bytes
  NUM_SECTORS=$(mac_hfs_provision_sectors ${BYTE_SIZE} 512)

  # Format the RAM FS
  # We may have a pre-existing RAM FS which we need to throw away
  echo "Formatting RamFS: ${F} ${NUM_SECTORS} sectors (${MEM_SIZE})."
  DEVICE=$(df -l | grep ${F} | cut -d " " -f 1)
  if [[ -n "${DEVICE}" ]]; then
    hdiutil detach -force ${DEVICE}
  fi
  diskutil erasevolume HFS+ ${F} $(hdiutil attach -nomount ram://${NUM_SECTORS})
}

function mount_local() {
  if [[ $(uname -a) == Darwin* ]]; then
    # Assuming Mac OS X
    mount_ramfs_mac
  else
    # Assuming Linux
    if [[ "$1" == "SudoMount" ]]; then
      DECL_INIT=$(declare -f init_env)
      DECL_MEM_SIZE_TO_BYTES=$(declare -f mem_size_to_bytes)
      DECL_MOUNT_LINUX=$(declare -f mount_ramfs_linux)
      sudo bash -O extglob -c "BIN=${BIN}; ${DECL_INIT}; ${DECL_MEM_SIZE_TO_BYTES}; \
       ${DECL_MOUNT_LINUX}; mount_ramfs_linux $0"
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
        ${LAUNCHER} ${BIN}/alluxio-workers.sh ${BIN}/alluxio-mount.sh $1
        ;;
    esac
    ;;
  *)
    echo -e ${USAGE}
    exit 1
esac
