#!/bin/sh  
echo "127.0.0.1 gfs-vm" >> /etc/hosts
mkdir -p /gfs_vol
mkdir -p /vol
service glusterd start
gluster volume create gfs_vol gfs-vm:/gfs_vol force
gluster volume start gfs_vol
mount -t glusterfs localhost:gfs_vol /vol
