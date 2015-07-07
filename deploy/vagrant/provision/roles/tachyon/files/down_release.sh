#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST=/vagrant/shared/$TACHYON_DIST

if [ ! -f $DIST ]; then
 version=`echo $DIST | cut -d'-' -f2`
 sudo yum install -y -q wget
 wget -q https://github.com/amplab/tachyon/releases/download/v${version}/${TACHYON_DIST} -P /vagrant/shared
fi

tar xzf $DIST -C /tachyon --strip-components 1
