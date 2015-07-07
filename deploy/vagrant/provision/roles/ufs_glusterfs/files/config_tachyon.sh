#!/bin/bash

# last should be master
namenode=`tail -n1 /tachyon/conf/workers`

# config tachyon env
/bin/cp -f /tachyon/conf/tachyon-glusterfs-env.sh.template /tachyon/conf/tachyon-env.sh
sed -i "s/export TACHYON_MASTER_ADDRESS=localhost/export TACHYON_MASTER_ADDRESS=${namenode}/g" /tachyon/conf/tachyon-env.sh
