#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST=/vagrant/shared/$TACHYON_DIST

if [ ! -f $DIST ]; then
 version=`echo $DIST | cut -d'-' -f2`
 sudo yum install -y -q wget
 wget -q http://tachyon-project.org/downloads/files/${version}/${TACHYON_DIST} -P /vagrant/shared
 if [ $? -ne 0 ]; then
     echo "Failed to download alluxio distribution $TACHYON_DIST. Please " \
          "make sure your alluxio and hadoop versions are valid"
     exit 1
 fi
fi
