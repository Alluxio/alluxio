#!/bin/sh
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


set -e

nodes=$(cat /vagrant/files/workers)
vol="alluxio_vol"
brick=""

# create a simple volume
for i in ${nodes[@]}; do
  sudo gluster peer probe ${i}
done

for i in ${nodes[@]}; do
  if [[ "x${brick}" == "x" ]]; then
    sudo gluster volume create ${vol} ${i}:/gfs_vol force
  else
    sudo gluster volume add-brick ${vol} ${i}:/gfs_vol force
  fi
  brick=${i}
done

# start volume
sudo gluster volume start ${vol}

# mount volume
for i in ${nodes[@]}; do
  ssh ${i} "sudo mount -t glusterfs localhost:${vol} /vol"
done
