#!/bin/sh
set -e

nodes=$(cat /vagrant/files/workers)
vol="alluxio_vol"
brick=""

# create a simple volume
for i in ${nodes[@]}; do
 sudo gluster peer probe ${i}
done

for i in ${nodes[@]}; do
 if [ "x${brick}" == "x" ]; then
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
