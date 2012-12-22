#!/bin/bash

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [ -e $TACHYON_HOME/conf/tachyon-env.sh ] ; then
  . $TACHYON_HOME/conf/tachyon-env.sh
fi

if [ -z $TACHYON_RAM_FOLDER ] ; then
  echo "TACHYON_RAM_FOLDER was not set. Using the default one: /mnt/ramfs"
  TACHYON_RAM_FOLDER=/mnt/ramfs
fi

F=$TACHYON_RAM_FOLDER

echo "Formatting RamFS: $F"
mkdir -p $F; mount -t ramfs -o size=15g ramfs $F ; chmod a+w $F ;