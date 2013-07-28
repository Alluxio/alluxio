#!/bin/bash

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [ -e $TACHYON_HOME/conf/tachyon-env.sh ] ; then
  . $TACHYON_HOME/conf/tachyon-env.sh
fi

if [ -z $TACHYON_RAM_FOLDER ] ; then
  TACHYON_RAM_FOLDER=/mnt/ramdisk
  echo "TACHYON_RAM_FOLDER was not set. Using the default one: $TACHYON_RAM_FOLDER"
fi

F=$TACHYON_RAM_FOLDER

# Lower case memory size.
MEM_SIZE=$(echo "$TACHYON_WORKER_MEMORY_SIZE" | tr -s '[:upper:]' '[:lower:]')

echo "Formatting RamFS: $F ($MEM_SIZE)"
umount -f $F ; mkdir -p $F; mount -t ramfs -o size=$MEM_SIZE ramfs $F ; chmod a+w $F ;
