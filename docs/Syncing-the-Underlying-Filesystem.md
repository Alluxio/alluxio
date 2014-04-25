---
layout: global
title: Syncing the Underlayer Filesystem
---

Often times, there is already data in the underlying store, but when Tachyon is started, it will not
have knowledge about the preexisting files.

Use the tachyon shell command loadufs to sync the filesystems.

    $ ./bin/tachyon loadufs [TACHYON_PATH] [UNDERLYING_FILESYSTEM_PATH] [-Optional EXCLUDE_PATHS]

For example:

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998 hdfs://localhost:9000 /tachyon

Would load the meta-data for all the files in the local hdfs, except for the Tachyon folder.

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998/locals file:///Users/tom /Users/tom/tachyon;/Users/tom/spark

Would load meta-data for all files in the local file under /Users/tom (except for tachyon and spark) to
address tachyon://127.0.0.1:19998/locals/Users/tom. The prefix "file://" can be safely omitted for local files.

In a sense, this is similar to the unix mount command. It's not called mount so as not to cause confusions.
Note that the optional EXCLUDE_PATHS are prefixes. Hence if you give something like "/logs/user.log", then
"/logs/master.log" will be loaded but not "/logs/user.log@12345".

