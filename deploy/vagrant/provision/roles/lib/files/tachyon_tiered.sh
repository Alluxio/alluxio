#!/usr/bin/env bash

DISK=`ls / | grep '^disk'`
TIERED_PATH=""
TIERED_QUOTA=""
for disk in $DISK; do
 TIERED_PATH=/$disk,$TIERED_PATH
 quota=`df -h | grep "/$disk" | awk '{print $2}'`
 TIERED_QUOTA=${quota}B,$TIERED_QUOTA
done

[[ "$TIERED_PATH" == "" ]] && exit 0

sed -i "s/tachyon.worker.tieredstore.levels=1/tachyon.worker.tieredstore.levels=2/g" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dtachyon.worker.tieredstore.level1.dirs.quota=$TIERED_QUOTA
" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dtachyon.worker.tieredstore.level1.dirs.path=$TIERED_PATH
" /tachyon/conf/tachyon-env.sh

sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dtachyon.worker.tieredstore.level1.alias=SSD
" /tachyon/conf/tachyon-env.sh
