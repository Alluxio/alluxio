#!/usr/bin/env bash

# remove the property from alluxio-env.sh if it exists and set it in alluxio-site.properties
sed -i "s/-Dalluxio.master.journal.folder=.*//g" /alluxio/conf/alluxio-env.sh
echo 'alluxio.master.journal.folder=${alluxio.underfs.address}/alluxio/journal/' >> /alluxio/conf/alluxio-site.properties
