---
layout: global
title: Syncing the Underlayer Filesystem
---

Often times, there is already data in the underlying store, but when Tachyon is started, it will not
have knowledge about the preexisting files.

Use the tachyon shell command loadufs to sync the filesystems.

`./bin/tachyon loadufs [TACHYON_ADDRESS] [UNDERLYING_FILESYSTEM_ADDRESS] [ROOT_DIRECTORY] [-Optional EXCLUDE_PATHS]`

For example:

`./bin/tachyon loadufs 127.0.0.1:19998 hdfs://localhost:8020 / /tachyon`

Would load the meta-data for all the files in the local hdfs, except for the Tachyon folder.
