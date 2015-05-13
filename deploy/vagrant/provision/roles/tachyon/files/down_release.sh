#!/usr/bin/env bash

# ANSIBLE_TACHYON_DIST is set to variable {{ tachyon_dist }} in this file
source /tmp/tachyon_dist.sh

mkdir -p /vagrant/shared

DIST=/vagrant/shared/$ANSIBLE_TACHYON_DIST

if [ ! -f $DIST ]; then
 version=`echo $DIST | cut -d'-' -f2`
 sudo yum install -y -q wget
 wget -q https://github.com/amplab/tachyon/releases/download/v${version}/${ANSIBLE_TACHYON_DIST} -P /vagrant/shared
fi

tar xzf $DIST -C /tachyon --strip-components 1

