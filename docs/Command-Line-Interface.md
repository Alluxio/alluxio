---
layout: global
title: Command Line Interface
group: Features
priority: 0
---

Tachyon's command line interface provides users with basic file system operations. You can invoke
the command line utility using:

```bash
$ ./bin/tachyon tfs
```

All "path" variables in tfs commands should start with

    tachyon://<master node address>:<master node port>/<path>

Or, if no header is provided, the default hostname and port (set in the env file) will be used.

    /<path>

## Wildcard Input

Most of the commands which require path components allow wildcard arguments for ease of use. For
example:

```bash
$ ./bin/tachyon tfs rm /data/2014*
```

The example command would delete anything in the `data` directory with a prefix of `2014`.

Note that some shells will attempt to glob the input paths, causing strange errors (Note: the number
21 could be different and comes from the number of matching files in your local filesystem):

```
rm takes 1 arguments,  not 21
```

As a work around, you can disable globbing (depending on your shell, for example `set -f`) or by
escaping wildcards, for example:

```bash
$ ./bin/tachyon tfs cat /\\*
```
Note the double escape, this is because the shell script will eventually call a java program which
should have the final escaped parameters (cat /\\*).

# List of Operations

<table class="table table-striped">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  <tr>
    <td>cat</td>
    <td>cat "path"</td>
    <td>Print the content of the file to the console.</td>
  </tr>
  <tr>
    <td>copyFromLocal</td>
    <td>copyFromLocal "source path" "remote path"</td>
    <td>Copy the specified file specified by "source path" to the path specified by "remote path".
    This command will fail if "remote path" already exists.</td>
  </tr>
  <tr>
    <td>copyToLocal</td>
    <td>copyToLocal "remote path" "local path"</td>
    <td>Copy the specified file from the path specified by "remote path" to a local
    destination.</td>
  </tr>
  <tr>
    <td>count</td>
    <td>count "path"</td>
    <td>Display the number of folders and files matching the specified prefix in "path".</td>
  </tr>
  <tr>
    <td>du</td>
    <td>du "path"</td>
    <td>Display the size of a file or a directory specified by the input path.</td>
  </tr>
  <tr>
    <td>fileinfo</td>
    <td>fileinfo "path"</td>
    <td>Print the information of the blocks of a specified file.</td>
  </tr>
  <tr>
    <td>free</td>
    <td>free "path"</td>
    <td>Free a file or all files under a directory from Tachyon. If the file/directory is also
    in under storage, it will still be available there.</td>
  </tr>
  <tr>
    <td>getCapacityBytes</td>
    <td>getCapacityBytes</td>
    <td>Get the capacity of the TachyonFS.</td>
  </tr>
  <tr>
    <td>getUsedBytes</td>
    <td>getUsedBytes</td>
    <td>Get number of bytes used in the TachyonFS.</td>
  </tr>
  <tr>
    <td>load</td>
    <td>load "path"</td>
    <td>Load the data of a file or a directory from under storage into Tachyon.</td>
  </tr>
  <tr>
    <td>loadMetadata</td>
    <td>loadMetadata "path"</td>
    <td>Load the metadata of a file or a directory from under storage into Tachyon.</td>
  </tr>
  <tr>
    <td>location</td>
    <td>location "path"</td>
    <td>Display a list of hosts that have the file data.</td>
  </tr>
  <tr>
    <td>ls</td>
    <td>ls "path"</td>
    <td>List all the files and directories directly under the given path with information such as
    size.</td>
  </tr>
  <tr>
    <td>lsr</td>
    <td>lsr "path"</td>
    <td>Recursively list all the files and directories under the given path with information such
    as size.</td>
  </tr>
  <tr>
    <td>mkdir</td>
    <td>mkdir "path1" ... "pathn" </td>
    <td>Create directory(ies) under the given paths, along with any necessary parent directories. Multiple paths separated by spaces or tabs. This
    command will fail if any of the given paths already exist.</td>
  </tr>
  <tr>
    <td>mount</td>
    <td>mount "path" "uri"</td>
    <td>Mount the underlying file system path "uri" into the Tachyon namespace as "path". The "path"
    is assumed not to exist and is created by the operation. No data or metadata is loaded from
    under storage into Tachyon. After a path is mounted, operations on objects under the mounted
    path are mirror to the mounted under storage.</td>
  </tr>
  <tr>
    <td>mv</td>
    <td>mv "source" "destination"</td>
    <td>Move a file or directory specified by "source" to a new location "destination". This command
    will fail if "destination" already exists.</td>
  </tr>
  <tr>
    <td>persist</td>
    <td>persist "path"</td>
    <td>Persist a file or directory currently stored only in Tachyon to the underlying file system.
    </td>
  </tr>
  <tr>
    <td>pin</td>
    <td>pin "path"</td>
    <td>Pin the given file to avoid evicting it from memory. If the given path is a directory, it
    recursively pins all the files contained and any new files created within this directory.</td>
  </tr>
  <tr>
    <td>report</td>
    <td>report "path"</td>
    <td>Report to the master that a file is lost.</td>
  </tr>
  <tr>
    <td>rm</td>
    <td>rm "path"</td>
    <td>Remove a file. This command will fail if the given path is a directory rather than a
    file.</td>
  </tr>
  <tr>
    <td>rmr</td>
    <td>rmr "path"</td>
    <td>Remove a file, or a directory with all the files and sub-directories that this directory
    contains.</td>
  </tr>
  <tr>
    <td>setTtl</td>
    <td>setTtl "time"</td>
    <td>Set the TTL (time to live) in milliseconds to a file.</td>
  </tr>
  <tr>
    <td>tail</td>
    <td>tail "path"</td>
    <td>Print the last 1KB of the specified file to the console.</td>
  </tr>
  <tr>
    <td>touch</td>
    <td>touch "path"</td>
    <td>Create a 0-byte file at the specified location.</td>
  </tr>
  <tr>
    <td>unmount</td>
    <td>unmount "path"</td>
    <td>Unmount the underlying file system path mounted in the Tachyon namespace as "path". Tachyon
    objects under "path" are removed from Tachyon, but they still exist in the previously mounted
    under storage.</td>
  </tr>
  <tr>
    <td>unpin</td>
    <td>unpin "path"</td>
    <td>Unpin the given file to allow Tachyon to evict this file again. If the given path is a
    directory, it recursively unpins all files contained and any new files created within this
    directory.</td>
  </tr>
  <tr>
    <td>unsetTtl</td>
    <td>unsetTtl</td>
    <td>Remove the TTL (time to live) setting from a file.</td>
  </tr>
</table>

# Example Use Cases

## cat
The `cat` command prints the entire contents of a file in Tachyon to the console. This can be
useful for verifying the file is what the user expects. If you wish to copy the file to your local
file system, `copyToLocal` should be used.

For example, when trying out a new computation job, `cat` can be used as a quick way to check the
output:

```bash
$ ./bin/tachyon tfs cat /output/part-00000
```

## copyFromLocal
The `copyFromLocal` command copies the contents of a file in your local file system into Tachyon.
If the node you run the command from has a Tachyon worker, the data will be available on that
worker. Otherwise, the data will be placed in a random remote node running a Tachyon worker. If a
directory is specified, the directory and all its contents will be uploaded recursively.

For example, `copyFromLocal` can be used as a quick way to inject data into the system for
processing:

```bash
$ ./bin/tachyon tfs copyFromLocal /local/data /input
```

## copyToLocal
The `copyToLocal` command copies the contents of a file in Tachyon to a file in your local file
system. If a directory is specified, the directory and all its contents will be downloaded
recurisvely.

For example, `copyToLocal` can be used as a quick way to download output data for additional
investigation or debugging.

```bash
$ ./bin/tachyon tfs copyToLocal /output/part-00000 part-00000
$ wc -l part-00000
```

## count
The `count` command outputs the number of files and folders matching a prefix as well as the
total size of the files. `Count` works recursively and accounts for any nested directories and
files. `Count` is best utilized when the user has some predefined naming conventions for their
files.

For example, if data files are stored by their date, `count` can be used to determine the number of
data files and their total size for any date, month, or year.

```bash
$ ./bin/tachyon tfs count /data/2014
```

## du
The `du` command outputs the size of a file. If a directory is specified, it will output the
aggregate size of all files in the directory and its children directories.

For example, if the Tachyon space is unexpectedly over utilized, `du` can be used to detect
which folders are taking up the most space.

```bash
$ ./bin/tachyon tfs du /\\*
```

## fileinfo
The `fileinfo` command dumps the FileInfo representation of a file to the console. It is primarily
intended to assist powerusers in debugging their system. Generally viewing the file info in the UI
will be much easier to understand.

For example, `fileinfo` can be used to debug the block locations of a file. This is useful when
trying to achieve locality for compute workloads.

```bash
$ ./bin/tachyon tfs fileinfo /data/2015/logs-1.txt
```

## free
The `free` command sends a request to the master to evict all blocks of a file from the Tachyon
workers. If the argument to `free` is a directory, it will recursively `free` all files. This
request is not guaranteed to take effect immediately, as readers may be currently using the blocks
of the file. `Free` will return immediately after the request is acknowledged by the master. Note
that `free` does not delete any data from the under storage system, and only affects data stored in
Tachyon space. In addition, metadata will not be affected by this operation, meaning the freed file
will still show up if an `ls` command is run.

For example, `free` can be used to manually manage Tachyon's data caching.

```bash
$ ./bin/tachyon tfs free /unused/data
```

## getCapacityBytes
The `getCapacityBytes` command returns the maximum number of bytes Tachyon is configured to store.

For example, `getCapacityBytes` can be used to verify if your cluster is set up as expected.

```bash
$ ./bin/tachyon tfs getCapacityBytes
```

## getUsedBytes
The `getUsedBytes` command returns the number of used bytes in Tachyon.

For example, `getUsedBytes` can be used to monitor the health of your cluster.

```bash
$ ./bin/tachyon tfs getUsedBytes
```

## load
The `load` command moves data from the under storage system into Tachyon storage. If there is a
Tachyon worker on the machine this command is run from, the data will be loaded to that worker.
Otherwise, a random worker will be selected to serve the data. Load will no-op if the file is
already in Tachyon memory level storage. If `load` is run on a directory, files in the directory
will be recursively loaded.

For example, `load` can be used to prefetch data for analytics jobs.

```bash
$ ./bin/tachyon tfs load /data/today
```

## loadMetadata
The `loadMetadata` command queries the under storage system for any file or directory matching the
given path and then creates a mirror of the file in Tachyon backed by that file. Only the metadata,
such as the file name and size are loaded this way and no data transfer occurs.

For example, `loadMetadata` can be used when other systems output to the under storage directly
(bypassing Tachyon), and the application running on Tachyon needs to use the output of those
systems.

```bash
$ ./bin/tachyon tfs loadMetadata /hdfs/data/2015/logs-1.txt
```

## location
The `location` command returns the addresses of all the Tachyon workers which contain blocks
belonging to the given file.

For example, `location` can be used to debug data locality when running jobs using a compute
framework.

```bash
$ ./bin/tachyon tfs location /data/2015/logs-1.txt
```

## ls
The `ls` command lists all the immediate children in a directory and displays the file size, last
modification time, and in memory status of the files. Using `ls` on a file will only display the
information for that specific file.

For example, `ls` can be used to browse the file system.

```bash
$ ./bin/tachyon tfs ls /users/alice/
```

## lsr
The `lsr` command is similar to `ls`, but it also recursively lists child directories, displaying
the entire subtree starting from the input path. As with `ls`, using `lsr` on a file will only
display information for that specific file.

For example, `lsr` can be used to browse the file system.

```bash
$ ./bin/tachyon tfs lsr /users/
```

## mkdir
The `mkdir` command creates a new directory in Tachyon space. It is recursive and will create any
nonexistent parent directories. Note that the created directory will not be created in the under
storage system until a file in the directory is persisted to the underlying storage. Using `mkdir`
on an invalid or already existing path will fail.

For example, `mkdir` can be used by an admin to set up the basic folder structures.

```bash
$ ./bin/tachyon tfs mkdir /users
$ ./bin/tachyon tfs mkdir /users/Alice
$ ./bin/tachyon tfs mkdir /users/Bob
```

## mount
The `mount` command links an under storage path to a Tachyon path, and files and folders created
in Tachyon space under the path will be backed by a corresponding file or folder in the under
storage path. For more details, see [Unified Namespace](Unified-and-Transparent-Namespace.html).

For example, `mount` can be used to make data in another storage system available in Tachyon.

```bash
$ ./bin/tachyon tfs mount s3n://data-bucket/ /s3/data
```

## mv
The `mv` command moves a file or directory to another path in Tachyon. The destination path must not
exist or be a directory. If it is a directory, the file or directory will be placed as a child of
the directory. `mv` is purely a metadata operation and does not affect the data blocks of the file.
`mv` cannot be done between mount points of different under storage systems.

For example, `mv` can be used to move older data into a non working directory.

```bash
$ ./bin/tachyon tfs mv /data/2014 /data/archives/2014
```

## persist
The `persist` command persists data in Tachyon storage into the under storage system. This is a data
operation and will take time depending on how large the file is. After persist is complete, the file
in Tachyon will be backed by the file in the under storage, make it still valid if the Tachyon
blocks are evicted or otherwise lost.

For example, `persist` can be used after filtering a series of temporary files for the ones
containing useful data.

```bash
$ ./bin/tachyon tfs persist /tmp/experimental-logs-2.txt
```

## pin
The `pin` command marks a file or folder as pinned in Tachyon. This is a metadata operation and will
not cause any data to be loaded into Tachyon. If a file is pinned, any blocks belonging to the file
will never be evicted from a Tachyon worker. If there are too many pinned files, Tachyon workers may
run low on storage space preventing other files from being cached.

For example, `pin` can be used to manually ensure performance if the administrator understands the
workloads well.

```bash
$ ./bin/tachyon tfs pin /data/today
```

## report
The `report` command marks a file as lost to the Tachyon master. This command should only be used
with files created using the [Lineage API](Lineage-API.html). Marking a file as lost will cause the
master to schedule a recomputation job to regenerate the file.

For example, `report` can be used to force recomputation of a file.

```bash
$ ./bin/tachyon tfs report /tmp/lineage-file
```

## rm
The `rm` command removes a file from Tachyon space and the under storage system. The file will be
unavailable immediately after this command returns, but the actual data may be deleted a while
later.

For example, `rm` can be used to remove temporary files which are no longer needed.

```bash
$ ./bin/tachyon tfs rm /tmp/unused-file
```

## rmr
The `rmr` command is similar to `rm`, but can also take a directory as an argument. `Rmr` will
delete all contents of the directory and then the directory itself.

For example, `rmr` can be used to clean up entire subtrees in the Tachyon.

```bash
$ ./bin/tachyon tfs rmr /tmp/tests
```

## setTtl
The `setTtl` command sets the time-to-live of a file, in milliseconds. The file will automatically
be deleted once the current time is greater than the TTL + creation time of the file. This delete
will affect both Tachyon and the under storage system.

For example, `setTtl` can be used to clean up files the administrator knows are unnecessary after a
period of time.

```bash
$ ./bin/tachyon tfs setTtl /data/good-for-one-day 86400000
```

## tail
The `tail` command outputs the last 1 kb of data in a file to the console.

For example, `tail` can be used to verify the output of a job is in the expected format or contains
expected values.

```bash
$ ./bin/tachyon tfs tail /output/part-00000
```

## touch
The `touch` command creates a 0-byte file. Files created with `touch` cannot be overwritten and are
mostly useful as flags.

For example, `touch` can be used to create a file signifying the compeletion of analysis on a
directory.

```bash
$ ./bin/tachyon tfs touch /data/yesterday/_DONE_
```

## unmount
The `unmount` command disassociates a Tachyon path with an under storage directory. Tachyon metadata
for the mount point will be removed along with any data blocks, but the under storage system will
retain all metadata and data. See [Unified Namespace](Unified-and-Transparent-Namespace.html) for
more dtails.

For example, `unmount` can be used to remove an under storage system when the users no longer need
data from that system.

```bash
$ ./bin/tachyon tfs unmount /s3/data
```

## unpin
The `unpin` command unmarks a file or directory in Tachyon as pinned. This is a metadata operation
and will not evict or delete any data blocks. Once a file is unpinned, its data blocks can be
evicted from the various Tachyon workers containing the block.

For example, `unpin` can be used when the administrator knows there is a change in the data access
pattern.

```bash
$ ./bin/tachyon tfs unpin /data/yesterday/join-table
```

## unsetTtl
The `unsetTtl` command will remove the TTL of a file in Tachyon. This is a metadata operation and
will not evict or store blocks in Tachyon. The TTL of a file can later be reset with `setTtl`.

For example, `unsetTtl` can be used if a regularly managed file requires manual management due to
some special case.

```bash
$ ./bin/tachyon tfs unsetTtl /data/yesterday/data-not-yet-analyzed
```