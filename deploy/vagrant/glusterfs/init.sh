wget http://download.gluster.org/pub/gluster/glusterfs/3.5/LATEST/CentOS/glusterfs-epel.repo -O /etc/yum.repos.d/glusterfs-epel.repo
yum install -y glusterfs-server glusterfs-client

echo "127.0.0.1 gfs-vm" >> /etc/hosts
mkdir -p /gfs_vol
mkdir -p /vol
service glusterd start
gluster volume create gfs_vol gfs-vm:/gfs_vol force
gluster volume start gfs_vol
mount -t glusterfs localhost:gfs_vol /vol

echo "Now building package !!!!!!!!!!!!!"
cd /tachyon


mvn clean package install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0  
