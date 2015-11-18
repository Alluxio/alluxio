#!/bin/bash

# prepare Tachyon dir structure for a YARN container to launch master or worker
tar zxf tachyon.tar.gz

# launch master or worker on this container
if [ $1 = 'master' ]; then
  ./integration/bin/tachyon-master-yarn.sh
else
  ./integration/bin/tachyon-worker-yarn.sh
fi