#!/usr/bin/env bash

# For Tachyon version that is no less than 0.8
echo 'tachyon.worker.heartbeat.timeout.ms=1000000' >> /tachyon/conf/tachyon-site.properties

# For earlier Tachyon versions
sed -i "/export TACHYON_JAVA_OPTS+=\"/ a\
  -Dtachyon.worker.heartbeat.timeout.ms=1000000
" /tachyon/conf/tachyon-env.sh
