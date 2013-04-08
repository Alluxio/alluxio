#!/bin/bash

bin=`cd "$( dirname "$0" )"; pwd`

# Load the Tachyon configuration
. "$bin/tachyon-config.sh"

if [ -e $TACHYON_HOME/conf/tachyon-env.sh ] ; then
  . $TACHYON_HOME/conf/tachyon-env.sh
fi

if [ -z $TACHYON_RAM_FOLDER ] ; then
  TACHYON_RAM_FOLDER=/Volumes/tachyon
  echo "TACHYON_RAM_FOLDER was not set. Using the default one: $TACHYON_RAM_FOLDER"
fi

if [[ $TACHYON_RAM_FOLDER != "/Volumes/"* ]]; then
  echo "Invalid TACHYON_RAM_FOLDER: $TACHYON_RAM_FOLDER"
  echo "TACHYON_RAM_FOLDER must set to /Volumes/[name] on Mac OS X."
  exit 1
fi

# Remove the "/Volumes/" part so we can get the name of the volume.
F=${TACHYON_RAM_FOLDER/#\/Volumes\//}

# Lower case memory size.
MEM_SIZE=$(echo "$TACHYON_WORKER_MEMORY_SIZE" | tr -s '[:upper:]' '[:lower:]')

# Convert the memory size to number of sectors. Each sector is 512 Byte.
if [[ $MEM_SIZE == *"gb" ]]; then
  # Size was specified in gigabytes.
  SIZE_IN_GB=${MEM_SIZE/%gb/}
  NUM_SECTORS=$(($SIZE_IN_GB * 1024 * 2048))
elif [[ $MEM_SIZE == *"mb" ]]; then
  # Size was specified in megabytes.
  SIZE_IN_MB=${MEM_SIZE/%mb/}
  NUM_SECTORS=$(($SIZE_IN_MB * 2048))
elif [[ $MEM_SIZE == *"kb" ]]; then
  # Size was specified in kilobytes.
  SIZE_IN_KB=${MEM_SIZE/%kb/}
  NUM_SECTORS=$(($SIZE_IN_KB * 2))
elif [[ "$MEM_SIZE" =~ ^[0-9]+$ ]] ; then
  # Size was specified in bytes.
  NUM_SECTORS=$((MEM_SIZE / 512))
else
  echo "Please specify TACHYON_WORKER_MEMORY_SIZE in a correct form."
  exit 1
fi

echo "Formatting RamFS: $F ($NUM_SECTORS sectors)."
diskutil unmount /Volumes/$F
diskutil erasevolume HFS+ $F `hdiutil attach -nomount ram://$NUM_SECTORS`
