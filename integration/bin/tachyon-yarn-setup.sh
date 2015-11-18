#!/bin/bash
#
# Usage:
#  tachyon-yarn.sh <deployTachyonHome> <numWorkers> <pathHdfs>

tar zxf tachyon.tar.gz

pwd

ls -rl

if [ $1 = 'master' ]; then
  ./integration/bin/tachyon-master-yarn.sh
else
  ./integration/bin/tachyon-worker-yarn.sh
fi