#!/bin/bash

# create tachyon env
/bin/cp /tachyon/conf/tachyon-env.sh.template /tachyon/conf/tachyon-env.sh

sed -i "s/^export TACHYON_MASTER_ADDRESS=.*/export TACHYON_MASTER_ADDRESS=$(tail -n1 /tachyon/conf/workers)/g" /tachyon/conf/tachyon-env.sh
