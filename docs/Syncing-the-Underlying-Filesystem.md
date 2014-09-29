---
layout: global
title: Syncing the Underlayer Filesystem
---

Often, there is already data in the underlying store, but when Tachyon is started, it will not have
knowledge about these preexisting files.

Use the tachyon shell command loadufs to sync the filesystems.

    $ ./bin/tachyon loadufs [TACHYON_PATH] [UNDERLYING_FILESYSTEM_PATH] [Optional EXCLUDE_PATHS]

For example:

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998 hdfs://localhost:9000 tachyon

Would load the meta-data for all the files in the local hdfs, except for the tachyon folder.

    $ ./bin/tachyon loadufs tachyon://127.0.0.1:19998/tomlogs file:///Users/tom/logs "tachyon;spark"

Would load the metadata for all local files under the /Users/tom/logs directory (except for tachyon
and spark) to the address tachyon://127.0.0.1:19998/tomlogs. If /Users/tom/logs itself is a file,
only that file is loaded as /tomlogs/logs in the TFS. The prefix "file://" can be safely omitted for
a local file system.

Note that the optional EXCLUDE_PATHS are prefixes relative to the given local file path. Moreover,
only files matching the given prefixes relative to the path will be excluded. Hence, in the above
last example, logs/tachyon and logs/spark will be excluded, but not logs/shark/tachyon nor
logs/shark/spark. To exclude these two paths as well, the exclude list should be specified as
"tachyon;spark;shark/tachyon;shark/spark". It is important to note that when ";" is present to
concatenate multiple prefixes, they must be surrounded by quotes; otherwise it would be treated as
multiple commands to be executed sequentially.

In a sense, loadufs is similar to the unix mount command. It's not called mount so as not to cause
confusion with the use of mount in the tachyon scripts.
