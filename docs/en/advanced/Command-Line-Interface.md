---
layout: global
title: Command Line Interface
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

Alluxio's command line interface provides users with basic file system operations. You can invoke
the following command line utility to get all the subcommands:

```bash
$ ./bin/alluxio
Usage: alluxio [COMMAND]
       [format [-s]]
       [getConf [key]]
       [logLevel]
       [runTests]
       ...
```

## General operations

In this section, we list the usages and examples of general Alluxio operations except file system 
and file system admin operations. File system admin operations are listed in the [Admin CLI doc](Admin-CLI.html).

### format

The `format` command will format the Alluxio master and all workers. 

If you run this command for an existing Alluxio cluster, all data and metadata stored in Alluxio will be deleted. 
However, data in under storage will not be changed.

If `-s` specified, only format if underfs is local and doesn't already exist.

Warning: `format` is required when you run Alluxio for the first time. 
`format` should only be called while the cluster is not running.

Warning: `format` will delete all Alluxio data and metadata.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio format -s
```

### formatMaster

The `formatMaster` command will format the Alluxio master.

Alluxio master stores the metadata of the Alluxio related file system operations 
and files stored in Allxuio workers. If a under file system file is written through Alluxio 
or a user runs some `getStatus` related operations (e.g. `fs ls` command) on it,
Alluxio master will also store its metadata. `formatMaster` cleans the journal system 
and deletes all those metadata.

Warning: `formatMaster` should only be called while the cluster is not running.

Warning: `formatMaster` will delete all the metadata.

```bash
$ ./bin/alluxio formatMaster
```

### formatWorker 

The `formatWorker` command will format the Alluxio worker.

Unlike the Alluxio master stores all the metadata, Alluxio worker stores 
all the real data -- files and objects.

`formatWorker` will delete all the real data stored in this worker node.
However, data in under storage will not be changed.

Warning: `formatWorker` should only be called while the cluster is not running.

Warning: `formatWorker` will delete all the Alluxio data.

```bash
$ ./bin/alluxio formatWorker
```

### bootstrapConf

The `bootstrapConf` command will generate the bootstrap configuration file
`${ALLUXIO_HOME}/conf/alluxio-env.sh` with the specified `ALLUXIO_MASTER_HOSTNAME`, 
if the configuration file doesn't exist. 

In addition, worker memory size and ramdisk folder will be set in the config file
according to the machine status:
* type: Mac or Linux
* total memory size of that machine

```bash
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

### getConf

The `getConf` command will print the configured value for the given key. If the key is
invalid, the exit code will be nonzero. If the key is valid but isn't set, 
an empty string is printed. If no key is specified, all configuration is printed.

Options:

* `--master` option will print the configuration properties used by the master.
* `--source` option will print the source of the configuration properties as well.
* `--unit <arg>` option will display the values of the configuration in the given unit.
E.g., with "--unit KB", a configuration value of "4096B" will return 4, 
and with "--unit S", a configuration value of "5000ms" will return 5. 
Possible unit options include B, KB, MB, GB, TP, PB, MS, S, M, H, D.

```bash
# Displays all the current node configuration
$ ./bin/alluxio getConf

# Displays the value of a property key
$ ./bin/alluxio getConf alluxio.master.hostname

# Displays the configuration of the current running Alluxio leader master
$ ./bin/alluxio getConf --master

# Also display the source of the configuration
$ ./bin/alluxio getConf --source

# Displays the values in a given unit
$ ./bin/alluxio getConf --unit KB alluxio.user.block.size.bytes.default
$ ./bin/alluxio getConf --unit S alluxio.master.journal.flush.timeout
```

### logLevel

The `logLevel` command allows you to get or change the log level of a particular class on specific instances.
Our users are able to change Alluxio server-side log level at runtime using this command.

The syntax is `alluxio logLevel --logName=NAME [--target=<master|worker|host:port>] [--level=LEVEL]`.
* `--logName <arg>` indicates the logger's name (e.g.alluxio.master.file.DefaultFileSystemMaster)
* `--target <arg>` lists the Alluxio master or workers to set. The target could be `<master|workers|host:webPort>`. 
A list of targets separated by `,` can be specified. `host:webPort` pair must be one of the workers. 
Default target is master and all workers.
* `--level <arg>` If provided the command changes to the defined logger level, 
otherwise it gets and displays the current logger level.

For example, this command sets the class `alluxio.heartbeat.HeartbeatContext`'s logger level to DEBUG on master as well as a worker at `192.168.100.100:30000`.
```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=master,192.168.100.100:30000 --level=DEBUG
```

And the following command gets all workers' log level on class `alluxio.heartbeat.HeartbeatContext`
```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=workers
```

### runTests

The `runTests` command will run all end-to-end tests on an Alluxio cluster 
and provide a comprehensive sanity check.

```bash
$ ./bin/alluxio runTests
```

### upgradeJournal

The `upgradeJournal` command will upgrade an Alluxio journal version 0 (Alluxio version < 1.5.0) 
to an Alluxio journal version 1 (Alluxio version >= 1.5.0).

`-journalDirectoryV0 <arg>` will provide the v0 journal persisted location. It is assumed to be 
the same as the v1 journal directory if not set.

```bash
$ ./bin/alluxio upgradeJournal
```

### copyDir

The `copyDir` command will copy the `PATH` to all worker nodes listed in `conf/workers`.

```bash
$ ./bin/alluxio copyDir conf/alluxio-site.properties
```

### version

The `version` command will print Alluxio version and exit.

```bash
$ ./bin/alluxio version
```

### validateConf

The `validateConf` command will validate Alluxio conf and exit.

```bash
$ ./bin/alluxio validateConf
```

### validateEnv

Before starting Alluxio, you might want to make sure that your system environment is ready for running Alluxio services. 
You can run the `validateEnv` command to validate your environment and it will report potential problems 
that might prevent you from starting Alluxio services.

The usage is `validateEnv COMMAND [NAME] [OPTIONS]`. 

`COMMAND` can be one of the following values:
* `local` run all validation tasks on local
* `master` run master validation tasks on local
* `worker` run worker validation tasks on local
* `all` run corresponding validation tasks on all master nodes and worker nodes
* `masters` run master validation tasks on all master nodes
* `workers` run worker validation tasks on all worker nodes
* `list` list all validation tasks

```bash
# Runs all validation tasks on local
$ ./bin/alluxio validateEnv local

# Runs corresponding validation tasks on all master nodes and worker nodes
$ ./bin/alluxio validateEnv all

# Lists all validation tasks
$ ./bin/alluxio validateEnv list
```

For all commands except list, `NAME` can be any task full name or prefix.
When `NAME` is given, only tasks with name starts with the prefix will run.
If NAME is not given, all tasks for the given TARGET will run.

```bash
# Only run validation tasks that check your local system resource limits
$ ./bin/alluxio validateEnv ulimit
# Only run the tasks start with "ma", like "master.rpc.port.available" and "master.web.port.available"
$ ./bin/alluxio validateEnv local ma
```

`OPTIONS` can be a list of command line options. Each option has the format  `-<optionName> [optionValue]`
For example, `[-hadoopConfDir <arg>]` could set the path to server-side hadoop conf dir when running validating tasks.
 
## File System Operations

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

### head

The `head` command prints the first 1 kb of data in a file to the console.

Using the `-c [bytes]` option will print the first `n` bytes of data to the console.

```bash
$ ./bin/alluxio fs head -c 2048 /output/part-00000
```

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

Note that `--readonly` mounts are useful to prevent accidental write operations. If multiple 
Alluxio satellite clusters mount a remote storage cluster which serves as the central source of truth, 
`--readonly` option could help prevent any write operations, e.g. rm -R, on the satellite cluster 
from wiping out the remote storage.

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
Using the `-c [bytes]` option will print the last `n` bytes of data to the console.

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
