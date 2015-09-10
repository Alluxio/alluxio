#!/usr/bin/env bash

mkdir -p /vagrant/shared
version=`echo $TACHYON_DIST | cut -d'-' -f2`
rc=0

if [ $UFS == "hadoop2" ]; then
  if [ $HADOOP_TYPE == "apache" ]; then
    hadoop_version=`echo $HADOOP_VERSION | cut -d'.' -f1,2`
    TACHYON_DIST="tachyon-${version}-hadoop${hadoop_version}-bin.tar.gz"
  elif [ $HADOOP_TYPE == "cdh" ]; then
    TACHYON_DIST="tachyon-${version}-cdh4-bin.tar.gz"
  fi
fi

DIST=/vagrant/shared/$TACHYON_DIST

if [ ! -f $DIST ]; then
 sudo yum install -y -q wget
 wget -q https://github.com/amplab/tachyon/releases/download/v${version}/${TACHYON_DIST} -P /vagrant/shared
 rc=$?
fi

if [ $rc -eq 0 ]; then
    tar xzf $DIST -C /tachyon --strip-components 1
 else
    echo "ERROR: Your choice of UFS ${UFS}-${HADOOP_TYPE}-${HADOOP_VERSION} doesn't have a proper targeted Tachyon v${version} release download."
    echo "Please choose Tachyon Github Type with desired branch."
 fi
