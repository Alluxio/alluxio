---
layout: global
title: Syncing the Underlayer Filesystem
---

Often times, there is already data in the underlying store, but when Tachyon is started, it will
not have knowledge about the preexisting files.

Use the tachyon shell command loadufs to sync the filesystems.

    $ ./bin/tachyon loadufs [TACHYON_PATH] [UNDERLYING_FILESYSTEM_PATH] [Optional EXCLUDE_PATHS]

For example:

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998 hdfs://localhost:9000 tachyon

Would load the meta-data for all the files in the local hdfs, except for the tachyon folder.

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998/tomlogs file:///Users/tom/logs tachyon;spark

Would load meta-data for all local files under the /Users/tom/logs directory (except for tachyon
and spark) to address tachyon://127.0.0.1:19998/tomlogs. If /Users/tom/logs itself is a file, only
that file is loaded as /tomlogs/logs in the TFS. The prefix "file://" can be safely omitted for
a local file system.

Note that the optional EXCLUDE_PATHS are prefixes relative to the given local file path. Moreover,
only files matching the given prefixes at the first level will be excluded. Hence, logs/tachyon
and logs/spark will be excluded, but not logs/shark/spark nor logs/shark/spark.

In a sense, loadufs is similar to the unix mount command. It's not called mount so as not to cause
confusions with the use of mount in the tachyon scripts.

