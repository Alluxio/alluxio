#!/bin/bash

sed -i "s/^export TACHYON_UNDERFS_ADDRESS=.*/export TACHYON_UNDERFS_ADDRESS=hdfs:\/\/\$\{TACHYON_MASTER_ADDRESS\}:9000/g" /tachyon/conf/tachyon-env.sh
