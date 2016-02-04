#!/usr/bin/env bash

# remove the property from alluxio-env.sh if it exists and set it in alluxio-site.properties
sed -i "s/-Dtachyon.master.journal.folder=.*//g" /tachyon/conf/tachyon-env.sh
echo 'alluxio.master.journal.folder=${alluxio.underfs.address}/alluxio/journal/' >> /tachyon/conf/tachyon-site.properties
