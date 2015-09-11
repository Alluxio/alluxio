#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST=/vagrant/shared/$TACHYON_DIST

if [ ! -f $DIST ]; then
 version=`echo $DIST | cut -d'-' -f2`
 sudo yum install -y -q wget
 wget -q https://github.com/amplab/tachyon/releases/download/v${version}/${TACHYON_DIST} -P /vagrant/shared
 if [ $? -ne 0 ]; then
     echo "Failed to download tachyon distribution $TACHYON_DIST. Please " \
          "make sure your tachyon and hadoop versions are valid"
     exit 1
 fi
fi
