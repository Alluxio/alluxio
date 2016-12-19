#!/bin/bash
#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

# prepare Alluxio dir structure for a YARN container to launch master or worker
# the name of this tar file is referenced in code
tar zxf alluxio.tar.gz

CONTAINER_TYPE=$1
shift

# launch master or worker on this container
if [[ $CONTAINER_TYPE == 'alluxio-master' ]]; then
  ./integration/yarn/bin/alluxio-master-yarn.sh $@
elif [[ $CONTAINER_TYPE == 'alluxio-worker' ]]; then
  ./integration/yarn/bin/alluxio-worker-yarn.sh $@
elif [[ $CONTAINER_TYPE == 'application-master' ]]; then
  ./integration/yarn/bin/alluxio-application-master.sh $@
else
  echo "Unrecognized container type: $CONTAINER_TYPE" >&2
  exit 1
fi
