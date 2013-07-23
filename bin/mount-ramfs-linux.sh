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

echo "Formatting RamFS: $F"
umount -f $F; mkdir -p $F; mount -t ramfs -o size=1.5g ramfs $F ; chmod a+w $F ;
