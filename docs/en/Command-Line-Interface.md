---
layout: global
title: Command Line Interface
group: Features
priority: 0
---

* Table of Contents
{:toc}

Alluxio's command line interface provides users with basic file system operations. You can invoke
the following command line utility to get all the subcommands:

```bash
$ ./bin/alluxio fs
Usage: alluxio fs [generic options]
       [cat <path>]
       [checkConsistency [-r] <Alluxio path>]
       ...
```

For `fs` subcommands that take Alluxio URIs as argument (e.g. `ls`, `mkdir`), the argument should
be either a complete Alluxio URI `alluxio://<master-hostname>:<master-port>/<path>`, or `/<path>`
without header provided to use the default hostname and port set in the
`conf/allluxio-site.properties`.


>**Wildcard input**
>
>Most of the commands which require path components allow wildcard arguments for ease of use. For
>example:
>
>{% include Command-Line-Interface/rm.md %}
>
>The example command would delete anything in the `data` directory with a prefix of `2014`.
>
>Note that some shells will attempt to glob the input paths, causing strange errors (Note: the
>number 21 could be different and comes from the number of matching files in your local
>filesystem):
>
>{% include Command-Line-Interface/rm-error.md %}
>
>As a work around, you can disable globbing (depending on your shell, for example `set -f`) or by
>escaping wildcards, for example:
>
>{% include Command-Line-Interface/escape.md %}
>
>Note the double escape, this is because the shell script will eventually call a java program
>which should have the final escaped parameters (`cat /\\*`).

## List of Operations

<table class="table table-striped">
  <tr><th>Operation</th><th>Syntax</th><th>Description</th></tr>
  {% for item in site.data.table.operation-command %}
    <tr>
      <td>{{ item.operation }}</td>
      <td>{{ item.syntax }}</td>
      <td>{{ site.data.table.en.operation-command[item.operation] }}</td>
    </tr>
  {% endfor %}
</table>

## Example Use Cases

### cat

The `cat` command prints the entire contents of a file in Alluxio to the console. This can be useful for verifying the file is what the user expects. If you wish to copy the file to your local file system, `copyToLocal` should be used.

For example, when trying out a new computation job, `cat` can be used as a quick way to check the output:

{% include Command-Line-Interface/cat.md %}

### checkConsistency

The `checkConsistency` command compares Alluxio and under storage metadata for a given path. If the path is a directory, the entire subtree will be compared. The command returns a message listing each inconsistent file or directory. The system administrator should reconcile the differences of these files at their discretion. To avoid metadata inconsistencies between Alluxio and under storages, design your systems to modify files and directories through the Alluxio and avoid directly modifying state in the underlying storage.

If the `-r` option is used, the checkConsistency command will repair all inconsistent files and directories
under the given path. If an inconsistent file or directory exists only in under storage, its metadata will
be added to Alluxio. If an inconsistent file exists in Alluxio and its data is fully present in Alluxio, its metadata
will be loaded to Alluxio again.

NOTE: This command requires a read lock on the subtree being checked, meaning writes and updates
to files or directories in the subtree cannot be completed until this command completes.

For example, `checkConsistency` can be used to periodically validate the integrity of the namespace.

{% include Command-Line-Interface/checkConsistency.md %}

### checksum

The `checksum` command outputs the md5 value of a file in Alluxio.

For example, `checksum` can be used to verify the content of a file stored in Alluxio matches the content stored in an UnderFS or local filesystem:

{% include Command-Line-Interface/checksum.md %}

### chgrp

The `chgrp` command changes the group of the file or directory in Alluxio. Alluxio supports file authorization with Posix file permission. Group is an authorizable entity in Posix file permission model. The file owner or super-user can execute this command to change the group of the file or directory.

Adding `-R` option also changes the group of child file and child directory recursively.

For example, `chgrp` can be used as a quick way to change the group of file:

{% include Command-Line-Interface/chgrp.md %}

### chmod

The `chmod` command changes the permission of file or directory in Alluxio. Currently octal mode is supported: the numerical format accepts three octal digits which refer to permissions for the file owner, the group and other users. Here is number-permission mapping table:

Adding `-R` option also changes the permission of child file and child directory recursively.

<table class="table table-striped">
  <tr><th>Number</th><th>Permission</th><th>rwx</th></tr>
  {% for item in site.data.table.chmod-permission %}
    <tr>
      <td>{{ item.number }}</td>
      <td>{{ item.permission }}</td>
      <td>{{ item.rwx }}</td>
    </tr>
  {% endfor %}
</table>

For example, `chmod` can be used as a quick way to change the permission of file:

{% include Command-Line-Interface/chmod.md %}

### chown

The `chown` command changes the owner of the file or directory in Alluxio. For obvious security reasons, the ownership of a file can only be altered by a super-user.

Adding `-R` option also changes the owner of child file and child directory recursively.

For example, `chown` can be used as a quick way to change the owner of file:

{% include Command-Line-Interface/chown.md %}

### copyFromLocal

The `copyFromLocal` command copies the contents of a file in your local file system into Alluxio. If the node you run the command from has an Alluxio worker, the data will be available on that worker. Otherwise, the data will be copied to a random remote node running an Alluxio worker. If a directory is specified, the directory and all its contents will be copied recursively.

For example, `copyFromLocal` can be used as a quick way to inject data into the system for processing:

{% include Command-Line-Interface/copyFromLocal.md %}

### copyToLocal

The `copyToLocal` command copies the contents of a file in Alluxio to a file in your local file system. If a directory is specified, the directory and all its contents will be downloaded recursively.

For example, `copyToLocal` can be used as a quick way to download output data for additional investigation or debugging.

{% include Command-Line-Interface/copyToLocal.md %}

### count

The `count` command outputs the number of files and folders matching a prefix as well as the total size of the files. `count` works recursively and accounts for any nested directories and files. `count` is best utilized when the user has some predefined naming conventions for their files.

For example, if data files are stored by their date, `count` can be used to determine the number of data files and their total size for any date, month, or year.

{% include Command-Line-Interface/count.md %}

### cp

The `cp` command copies a file or directory in the Alluxio file system or between local file system
and Alluxio file system.

Scheme `file` indicates the local file system and scheme `alluxio` or no scheme indicates
the Alluxio file system.

If the `-R` option is used and the source designates a directory, cp copies the entire subtree at source to the destination.

For example, `cp` can be used to copy files between Under file systems.

{% include Command-Line-Interface/cp.md %}

### du

The `du` command outputs the size of a file. If a directory is specified, it will output the aggregate size of all files in the directory and its children directories.

For example, if the Alluxio space is unexpectedly over utilized, `du` can be used to detect which folders are taking up the most space.

{% include Command-Line-Interface/du.md %}

### fileInfo

The `fileInfo` command is deprecated since Alluxio version 1.5. Please use `alluxio fs stat <path>` command instead.

The `fileInfo` command dumps the FileInfo representation of a file to the console. It is primarily intended to assist powerusers in debugging their system. Generally viewing the file info in the UI will be much easier to understand.

For example, `fileInfo` can be used to debug the block locations of a file. This is useful when trying to achieve locality for compute workloads.

{% include Command-Line-Interface/fileInfo.md %}

### free

The `free` command sends a request to the master to evict all blocks of a file from the Alluxio workers. If the argument to `free` is a directory, it will recursively `free` all files. This request is not guaranteed to take effect immediately, as readers may be currently using the blocks of the file. `free` will return immediately after the request is acknowledged by the master. Note that, files must be persisted already in under storage before being freed, or the `free` command will fail; also any pinned files cannot be freed unless `-f` option is specified. The `free` command does not delete any data from the under storage system, but only removing the blocks of those files in Alluxio space to reclaim space. In addition, metadata will not be affected by this operation, meaning the freed file will still show up if an `ls` command is run.

For example, `free` can be used to manually manage Alluxio's data caching.

{% include Command-Line-Interface/free.md %}

### getCapacityBytes

The `getCapacityBytes` command returns the maximum number of bytes Alluxio is configured to store.

For example, `getCapacityBytes` can be used to verify if your cluster is set up as expected.

{% include Command-Line-Interface/getCapacityBytes.md %}

### getfacl
The `getfacl` command returns the ACL entries for a specified file or directory.

For example, `getfacl` can be used to verify that an ACL is changed successfully after a call to `setfacl`.

```bash
$ ./bin/alluxio fs getfacl /testdir/testfile
```

### getUsedBytes

The `getUsedBytes` command returns the number of used bytes in Alluxio.

For example, `getUsedBytes` can be used to monitor the health of your cluster.

{% include Command-Line-Interface/getUsedBytes.md %}

### help

The `help` command prints the help message for a given `fs` subcommand. If there isn't given
command, prints help messages for all supported subcommands.

Examples:

```bash
# Print all subcommands
$ ./bin/alluxio fs help
#
# Print help message for ls
$ ./bin/alluxio fs help ls
```

### leader

The `leader` command prints the current Alluxio leader master host name.

{% include Command-Line-Interface/leader.md %}

### load

The `load` command moves data from the under storage system into Alluxio storage. If there is a Alluxio worker on the machine this command is run from, the data will be loaded to that worker. Otherwise, a random worker will be selected to serve the data.
If the data is already loaded into Alluxio, load is a no-op unless the `--local flag` is used, it means that the `--local` flag will load data to a local worker even if the data is already available on remote workers. If `load` is run on a directory, files in the directory will be recursively loaded.

For example, `load` can be used to prefetch data for analytics jobs.

{% include Command-Line-Interface/load.md %}

### loadMetadata

The `loadMetadata` command is deprecated since Alluxio version 1.1.
Please use `alluxio fs ls <path>` command instead.

The `loadMetadata` command queries the under storage system for any file or directory matching the given path and then creates a mirror of the file in Alluxio backed by that file. Only the metadata, such as the file name and size are loaded this way and no data transfer occurs.

For example, `loadMetadata` can be used when other systems output to the under storage directly (bypassing Alluxio), and the application running on Alluxio needs to use the output of those systems.

{% include Command-Line-Interface/loadMetadata.md %}

### location

The `location` command returns the addresses of all the Alluxio workers which contain blocks belonging to the given file.

For example, `location` can be used to debug data locality when running jobs using a compute framework.

{% include Command-Line-Interface/location.md %}

### ls

The `ls` command lists all the immediate children in a directory and displays the file size, last modification time, and in memory status of the files. Using `ls` on a file will only display the information for that specific file.

The `ls` command will also load the metadata for any file or immedidate children of a directory
from the under storage system to Alluxio namespace, if it does not exist in Alluxio yet. `ls`
queries the under storage system for any file or directory matching the given path and then creates
a mirror of the file in Alluxio backed by that file. Only the metadata, such as the file name and
size are loaded this way and no data transfer occurs.

Options:

* `-d` option lists the directories as plain files. For example, `ls -d /` shows the atrributes
of root directory.
* `-f` option forces loading metadata for immediate children in a directory. By default, it loads
metadata only at the first time at which a directory is listed.
* `-h` option displays file sizes in human-readable formats.
* `-p` option lists all pinned files.
* `-R` option also recursively lists child directories, displaying the entire subtree starting from the input path.
* `--sort` sorts the result by the given option. Possible values: `size|creationTime|inMemoryPercentage|lastModificationTime|path`
* `-r` reverses the sorting order.

For example, `ls` can be used to browse the file system.

{% include Command-Line-Interface/ls.md %}

### masterInfo

The `masterInfo` command prints information regarding master fault tolerance such as leader address, list of master addresses, and the configured Zookeeper address. If Alluxio is running in single
master mode, `masterInfo` will print the master address. If Alluxio is running in fault tolerance mode,
the leader address, list of master addresses and the configured Zookeeper address will be printed.

For example, `masterInfo` can be used to print information regarding master fault tolerance.

{% include Command-Line-Interface/masterInfo.md %}

### mkdir

The `mkdir` command creates a new directory in Alluxio space. It is recursive and will create any nonexistent parent directories. Note that the created directory will not be created in the under storage system until a file in the directory is persisted to the underlying storage. Using `mkdir` on an invalid or already existing path will fail.

For example, `mkdir` can be used by an admin to set up the basic folder structures.

{% include Command-Line-Interface/mkdir.md %}

### mount

The `mount` command links an under storage path to an Alluxio path, and files and folders created in Alluxio space under the path will be backed by a corresponding file or folder in the under storage path. For more details, see [Unified Namespace](Unified-and-Transparent-Namespace.html).

Options:

* `--readonly` option sets the  mount point to be readonly in Alluxio
* `--option <key>=<val>` option passes an property to this mount point (e.g., S3 credential)

For example, `mount` can be used to make data in another storage system available in Alluxio.

{% include Command-Line-Interface/mount.md %}

### mv

The `mv` command moves a file or directory to another path in Alluxio. The destination path must not exist or be a directory. If it is a directory, the file or directory will be placed as a child of the directory. `mv` is purely a metadata operation and does not affect the data blocks of the file. `mv` cannot be done between mount points of different under storage systems.

For example, `mv` can be used to move older data into a non working directory.

{% include Command-Line-Interface/mv.md %}

### persist

The `persist` command persists data in Alluxio storage into the under storage system. This is a data operation and will take time depending on how large the file is. After persist is complete, the file in Alluxio will be backed by the file in the under storage, make it still valid if the Alluxio blocks are evicted or otherwise lost.

For example, `persist` can be used after filtering a series of temporary files for the ones containing useful data.

{% include Command-Line-Interface/persist.md %}

### pin

The `pin` command marks a file or folder as pinned in Alluxio. This is a metadata operation and will not cause any data to be loaded into Alluxio. If a file is pinned, any blocks belonging to the file will never be evicted from an Alluxio worker. If there are too many pinned files, Alluxio workers may run low on storage space preventing other files from being cached.

For example, `pin` can be used to manually ensure performance if the administrator understands the workloads well.

{% include Command-Line-Interface/pin.md %}

### rm

The `rm` command removes a file from Alluxio space and the under storage system. The file will be unavailable immediately after this command returns, but the actual data may be deleted a while later.

Add `-R` option will delete all contents of the directory and then the directory itself. Add `-U` option to not check whether the UFS contents being deleted are in-sync with Alluxio before attempting to delete persisted directories.

For example, `rm` can be used to remove temporary files which are no longer needed.

{% include Command-Line-Interface/rm2.md %}

### setfacl

The `setfacl` command modifies the access control list associated with a specified file or directory. 

The`-R` option will apply operations to all files and directories recursively.
The `-m` option will modify the ACL by adding/overwriting new entries.
The `-x` option will remove specific ACL entries.
The `-b` option will remove all ACL entries, except for the base entries.
The `-k` option will remove all the default ACL entries.

For example, `setfacl` can be used to give read and execute permissions to a user named `testuser`. 

```bash
$ ./bin/alluxio fs setfacl -m "user:testuser:r-x" /testdir/testfile
```

### setTtl

The `setTtl` command sets the time-to-live of a file or a directory, in milliseconds. If set ttl to a directory, all the children inside that directory will set too. So a directory's TTL expires, all the children inside that directory will also expire. Action parameter will indicate the action to perform once the current time is greater than the TTL + creation time of the file. Action `delete` (default) will delete file or directory from both Alluxio and the under storage system, whereas action `free` will just free the file from Alluxio even they are pinned.

For example, `setTtl` with action `delete` can be used to clean up files the administrator knows are unnecessary after a period of time, or with action `free` just remove the contents from Alluxio to make room for more space in Alluxio.

{% include Command-Line-Interface/setTtl.md %}

### stat

The `stat` command dumps the FileInfo representation of a file or a directory to the console. It is primarily intended to assist powerusers in debugging their system. Generally viewing the file info in the UI will be much easier to understand.

One can specify `-f <arg>` to display info in given format:
* "%N": name of the file;
* "%z": size of file in bytes;
* "%u": owner;
* "%g": group name of owner;
* "%y" or "%Y": modification time, %y shows 'yyyy-MM-dd HH:mm:ss' (the UTC
date), %Y it shows milliseconds since January 1, 1970 UTC;
* "%b": Number of blocks allocated for file

For example, `stat` can be used to debug the block locations of a file. This is useful when trying to achieve locality for compute workloads.

{% include Command-Line-Interface/stat.md %}

### tail

The `tail` command outputs the last 1 kb of data in a file to the console.

For example, `tail` can be used to verify the output of a job is in the expected format or contains expected values.

{% include Command-Line-Interface/tail.md %}

### test

The `test` command tests a property of a path, returning 0 if the property is true, or 1
otherwise. Specify `-d` to test whether the path is a directory, Specify `-f`
to test whether the path is a file, Specify `-e` to test whether the path
exists, Specify `-s` to test whether the directory is not empty, Specify `-z`
to test whether the file is zero length,

Options:

 * `-d` option tests whether path is a directory.
 * `-e` option tests whether path exists.
 * `-f` option tests whether path is a file.
 * `-s` option tests whether path is not empty.
 * `-z` option tests whether file is zero length.

Examples:

```bash
$ ./bin/alluxio fs test -d /someDir
$ echo $?
```

### touch

The `touch` command creates a 0-byte file. Files created with `touch` cannot be overwritten and are mostly useful as flags.

For example, `touch` can be used to create a file signifying the compeletion of analysis on a directory.

{% include Command-Line-Interface/touch.md %}

### unmount

The `unmount` command disassociates an Alluxio path with an under storage directory. Alluxio metadata for the mount point will be removed along with any data blocks, but the under storage system will retain all metadata and data. See [Unified Namespace](Unified-and-Transparent-Namespace.html) for more dtails.

For example, `unmount` can be used to remove an under storage system when the users no longer need data from that system.

{% include Command-Line-Interface/unmount.md %}

### unpin

The `unpin` command unmarks a file or directory in Alluxio as pinned. This is a metadata operation and will not evict or delete any data blocks. Once a file is unpinned, its data blocks can be evicted from the various Alluxio workers containing the block.

For example, `unpin` can be used when the administrator knows there is a change in the data access pattern.

{% include Command-Line-Interface/unpin.md %}

### unsetTtl

The `unsetTtl` command will remove the TTL of a file in Alluxio. This is a metadata operation and will not evict or store blocks in Alluxio. The TTL of a file can later be reset with `setTtl`.

For example, `unsetTtl` can be used if a regularly managed file requires manual management due to some special case.

{% include Command-Line-Interface/unsetTtl.md %}
