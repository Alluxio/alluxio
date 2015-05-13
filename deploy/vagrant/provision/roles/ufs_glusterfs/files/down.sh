#!/usr/bin/env bash

sudo wget -P /etc/yum.repos.d http://download.gluster.org/pub/gluster/glusterfs/LATEST/RHEL/glusterfs-epel.repo

sudo yum install -q -y glusterfs-server glusterfs-client
