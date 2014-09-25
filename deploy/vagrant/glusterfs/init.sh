#!/bin/sh

# install gluster
wget -q http://download.gluster.org/pub/gluster/glusterfs/3.5/LATEST/CentOS/glusterfs-epel.repo -O /etc/yum.repos.d/glusterfs-epel.repo
yum install -q -y glusterfs-server glusterfs-client

# config gluster
service glusterd start
mkdir -p /gfs_vol
mkdir -p /vol

# build tachyon pkg
cd /tachyon
mvn -q install -Dtest.profile=glusterfs -Dhadoop.version=2.3.0  -DskipTests
