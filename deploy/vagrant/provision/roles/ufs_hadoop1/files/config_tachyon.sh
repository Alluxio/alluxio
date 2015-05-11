#!/bin/bash

# last should be master
namenode=`tail -n1 /tachyon/conf/workers`

# create tachyon env
/bin/cp /tachyon/conf/tachyon-env.sh.template /tachyon/conf/tachyon-env.sh
sed -i "s/#export TACHYON_UNDERFS_ADDRESS=hdfs:\/\/localhost:9000/export TACHYON_UNDERFS_ADDRESS=hdfs:\/\/${namenode}:9000/g" /tachyon/conf/tachyon-env.sh
sed -i "s/export TACHYON_MASTER_ADDRESS=localhost/export TACHYON_MASTER_ADDRESS=${namenode}/g" /tachyon/conf/tachyon-env.sh
