#!/bin/bash

# prepare Tachyon dir structure for a YARN container to launch master or worker
# the name of this tar file is referenced in code
tar zxf tachyon.tar.gz

CONTAINER_TYPE=$1
shift

# launch master or worker on this container
if [ $CONTAINER_TYPE = 'alluxio-master' ]; then
  ./integration/bin/tachyon-master-yarn.sh $@
elif [ $CONTAINER_TYPE = 'alluxio-worker' ]; then
  ./integration/bin/tachyon-worker-yarn.sh $@
elif [ $CONTAINER_TYPE = 'application-master' ]; then
  ./integration/bin/tachyon-application-master.sh $@
else
  echo "Unrecognized container type: $CONTAINER_TYPE"
  exit 1
fi
