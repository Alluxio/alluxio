#!/usr/bin/env bash

set -e

sed -i "s/# export TACHYON_MASTER_BIND_HOST/export TACHYON_MASTER_BIND_HOST/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_MASTER_WEB_BIND_HOST/export TACHYON_MASTER_WEB_BIND_HOST/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_BIND_HOST/export TACHYON_WORKER_BIND_HOST/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_DATA_BIND_HOST/export TACHYON_WORKER_DATA_BIND_HOST/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_WEB_BIND_HOST/export TACHYON_WORKER_WEB_BIND_HOST/g" /tachyon/conf/tachyon-env.sh

sed -i "s/# export TACHYON_MASTER_HOSTNAME/export TACHYON_MASTER_HOSTNAME/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_MASTER_WEB_HOSTNAME/export TACHYON_MASTER_WEB_HOSTNAME/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_HOSTNAME/export TACHYON_WORKER_HOSTNAME/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_DATA_HOSTNAME/export TACHYON_WORKER_DATA_HOSTNAME/g" /tachyon/conf/tachyon-env.sh
sed -i "s/# export TACHYON_WORKER_WEB_HOSTNAME/export TACHYON_WORKER_WEB_HOSTNAME/g" /tachyon/conf/tachyon-env.sh
