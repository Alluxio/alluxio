#!/usr/bin/env bash

# For Tachyon version that is no less than 0.8
echo 'tachyon.master.journal.folder=${tachyon.underfs.address}/tachyon/journal/' >> /tachyon/conf/tachyon-site.properties

# For earlier Tachyon versions
sed -i "s/tachyon.master.journal.folder=.*/tachyon.master.journal.folder=\$TACHYON_UNDERFS_ADDRESS\/tachyon\/journal\//g" /tachyon/conf/tachyon-env.sh
