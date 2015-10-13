#!/usr/bin/env bash

# remove the property from tachyon-env.sh if it exists and set it in tachyon-site.properties
sed -i "s/-Dtachyon.master.journal.folder=.*//g" /tachyon/conf/tachyon-env.sh
echo 'tachyon.master.journal.folder=${tachyon.underfs.address}/tachyon/journal/' >> /tachyon/conf/tachyon-site.properties
