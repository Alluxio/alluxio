#!/usr/bin/env bash

mkdir -p /vagrant/shared

DIST=/vagrant/shared/${SPARK_DIST}

if [ ! -f ${DIST} ]; then
 version=$(echo ${DIST} | cut -d'-' -f2)
 sudo yum install -y -q wget
 wget -q http://archive.apache.org/dist/spark/spark-${version}/${SPARK_DIST} -P /vagrant/shared
fi

tar xzf ${DIST} -C /spark --strip-components 1
