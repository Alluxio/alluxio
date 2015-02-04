#!/usr/bin/env bash

# Start all Tachyon workers.
# Starts the master on this node.
# Starts a worker on each node specified in conf/workers

LAUNCHER=
# If debugging is enabled propagate that through to sub-shells
if [[ "$-" == *x* ]]; then
  LAUNCHER="bash -x"
fi

Usage="Usage: tachyon-mount.sh [Mount|SudoMount] [MACHINE]
\nIf omitted, MACHINE is default to be 'local'. MACHINE is one of:\n
  local\t\t\tMount local machine\n
  workers\t\tMount all the workers on worker nodes"

function init_env() {
  bin=`cd "$( dirname "$1" )"; pwd`

  DEFAULT_LIBEXEC_DIR="$bin"/../libexec
  TACHYON_LIBEXEC_DIR=${TACHYON_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
  . $TACHYON_LIBEXEC_DIR/tachyon-config.sh

  if [ -z $TACHYON_WORKER_MEMORY_SIZE ] ; then
    echo "TACHYON_WORKER_MEMORY_SIZE was not set. Using the default one: 128MB"
    TACHYON_WORKER_MEMORY_SIZE="128MB"
  fi

  MEM_SIZE=$(echo "$TACHYON_WORKER_MEMORY_SIZE" | tr -s '[:upper:]' '[:lower:]')
}

#enable the regexp case match
shopt -s extglob
function mem_size_to_bytes() {
  float_scale=2
  function float_eval() {
    local stat=0
    local result=0.0
    if [[ $# -gt 0 ]]; then
      result=$(echo "scale=$float_scale; $*" | bc -q 2>/dev/null)
      stat=$?
      if [[ $stat -eq 0  &&  -z "$result" ]]; then stat=1; fi
    fi
    echo $( printf "%.0f" $result )
    return $( printf "%.0f" $stat )
  }

  SIZE=${MEM_SIZE//[^0-9.]/}
  case ${MEM_SIZE} in
    *g?(b) )
      # Size was specified in gigabytes.
      BYTE_SIZE=$(float_eval "$SIZE * 1024 * 1024 * 1024")
      ;;
    *m?(b))
      # Size was specified in megabytes.
      BYTE_SIZE=$(float_eval "$SIZE * 1024 * 1024")
      ;;
    *k?(b))
      # Size was specified in kilobytes.
      BYTE_SIZE=$(float_eval "$SIZE * 1024")
      ;;
    +([0-9])?(b))
      # Size was specified in bytes.
      BYTE_SIZE=$SIZE
      ;;
    *)
      echo "Please specify TACHYON_WORKER_MEMORY_SIZE in a correct form."
      exit 1
  esac
}

function mount_ramfs_linux() {
  init_env $1

  if [ -z $TACHYON_RAM_FOLDER ] ; then
    TACHYON_RAM_FOLDER=/mnt/ramdisk
    echo "TACHYON_RAM_FOLDER was not set. Using the default one: $TACHYON_RAM_FOLDER"
  fi

  mem_size_to_bytes
  FREE_MEM=`free -b | grep "^Mem" | awk '{print $2}'`
  if [ $FREE_MEM -lt $BYTE_SIZE ] ; then
    echo "ERROR: Memory($FREE_MEM) is less than requested ramdisk size($BYTE_SIZE). Please reduce TACHYON_WORKER_MEMORY_SIZE"
    exit 1
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
  mem_size_to_bytes
  NUM_SECTORS=$((BYTE_SIZE / 512))

  # Format the RAM FS
  # We may have a pre-existing RAM FS which we need to throw away
  echo "Formatting RamFS: $F $NUM_SECTORS sectors ($MEM_SIZE)."
  DEVICE=`df -l | grep $F | cut -d " " -f 1`
  if [[ -n "${DEVICE}" ]]; then
    hdiutil detach -force $DEVICE
  fi
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
      DECL_MEM_SIZE_TO_BYTES=`declare -f mem_size_to_bytes`
      DECL_MOUNT_LINUX=`declare -f mount_ramfs_linux`
      sudo bash -O extglob -c "$DECL_INIT; $DECL_MEM_SIZE_TO_BYTES; $DECL_MOUNT_LINUX; mount_ramfs_linux $0"
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
        $LAUNCHER $bin/tachyon-workers.sh $bin/tachyon-mount.sh $1
        ;;
    esac
    ;;
  *)
    echo -e $Usage
    exit 1
esac
