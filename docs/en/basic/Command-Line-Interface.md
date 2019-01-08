---
layout: global
title: Command Line Interface
group: Basic
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

This section lists usages and examples of general Alluxio operations with the exception of file
system commands which are covered in the [Admin CLI doc]({{ '/en/operation/Admin-CLI.html' | relativize_url }}).

### format

The `format` command formats the Alluxio master and all its workers.

If `-s` specified, only format if under storage is local and does not already exist

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio,
including cached data and any metadata information.
Data in under storage will not be changed.

Warning: `format` is required when you run Alluxio for the first time.
`format` should only be called while the cluster is not running.

```bash
$ ./bin/alluxio format
$ ./bin/alluxio format -s
```

### formatMaster

The `formatMaster` command formats the Alluxio master.

The Alluxio master stores various forms of metadata, including:
- file system operations
- where files are located on workers
- journal transactions
- under storage file metadata

All this information is deleted if `formatMaster` is run.,

Warning: `formatMaster` should only be called while the cluster is not running.


```bash
$ ./bin/alluxio formatMaster
```

### formatWorker

The `formatWorker` command formats the Alluxio worker.

An Alluxio worker caches files and objects.

`formatWorker` deletes all the cached data stored in this worker node.
Data in under storage will not be changed.

Warning: `formatWorker` should only be called while the cluster is not running.

```bash
$ ./bin/alluxio formatWorker
```

### bootstrapConf

The `bootstrapConf` command generates the bootstrap configuration file
`${ALLUXIO_HOME}/conf/alluxio-env.sh` with the specified `ALLUXIO_MASTER_HOSTNAME`,
if the configuration file does not exist.

In addition, worker memory size and the ramdisk folder will be set in the configuration file
in accordance to the state of the machine:
* type: Mac or Linux
* total memory size

```bash
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

### getConf

The `getConf` command prints the configured value for the given key.
If the key is invalid, it returns a nonzero exit code.
If the key is valid but isn't set,  an empty string is printed.
If no key is specified, the full configuration is printed.

Options:

* `--master` option prints any configuration properties used by the master.
* `--source` option prints the source of the configuration properties.
* `--unit <arg>` option displays the configuration value in the given unit.
For example, with `--unit KB`, a configuration value of `4096B` returns as `4`,
and with `--unit S`, a configuration value of `5000ms` returns as `5`.
Possible unit options include B, KB, MB, GB, TP, PB as units of byte size and
MS, S, M, H, D as units of time.

```bash
# Displays all the current node configuration
$ ./bin/alluxio getConf

# Displays the value of a property key
$ ./bin/alluxio getConf alluxio.master.hostname

# Displays the configuration of the current running Alluxio leading master
$ ./bin/alluxio getConf --master

# Also display the source of the configuration
$ ./bin/alluxio getConf --source

# Displays the values in a given unit
$ ./bin/alluxio getConf --unit KB alluxio.user.block.size.bytes.default
$ ./bin/alluxio getConf --unit S alluxio.master.journal.flush.timeout
```

### logLevel

The `logLevel` command returns the current value of or updates the log level of a particular class
on specific instances. Users are able to change Alluxio server-side log levels at runtime.

The command follows the format `alluxio logLevel --logName=NAME [--target=<master|worker|host:port>] [--level=LEVEL]`,
where:
* `--logName <arg>` indicates the logger's class (e.g. `alluxio.master.file.DefaultFileSystemMaster`)
* `--target <arg>` lists the Alluxio master or workers to set.
The target could be of the form `<master|workers|host:webPort>` and multiple targets can be listed as comma-separated entries.
The `host:webPort` format can only be used when referencing a worker.
The default target value is all masters and workers.
* `--level <arg>` If provided, the command changes to the given logger level,
otherwise it returns the current logger level.

For example, the following command sets the logger level of the class `alluxio.heartbeat.HeartbeatContext` to
`DEBUG` on master as well as a worker at `192.168.100.100:30000`:

```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=master,192.168.100.100:30000 --level=DEBUG
```

And the following command returns the log level of the class `alluxio.heartbeat.HeartbeatContext` among all the workers:
```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=workers
```

### runTests

The `runTests` command runs end-to-end tests on an Alluxio cluster to provide a comprehensive sanity check.

```bash
$ ./bin/alluxio runTests
```

### upgradeJournal

The `upgradeJournal` command upgrades an Alluxio journal version 0 (Alluxio version < 1.5.0)
to an Alluxio journal version 1 (Alluxio version >= 1.5.0).

`-journalDirectoryV0 <arg>` will provide the v0 journal persisted location.\
It is assumed to be the same as the v1 journal directory if not set.

```bash
$ ./bin/alluxio upgradeJournal
```

### copyDir

The `copyDir` command copies the directory at `PATH` to all worker nodes listed in `conf/workers`.

```bash
$ ./bin/alluxio copyDir conf/alluxio-site.properties
```

### version

The `version` command prints Alluxio version.

```bash
$ ./bin/alluxio version
```

### validateConf

The `validateConf` command validates the local Alluxio configuration files, checking for common misconfigurations.

```bash
$ ./bin/alluxio validateConf
```

### validateEnv

Before starting Alluxio, it is recommended to ensure that the system environment is compatible with
running Alluxio services. The `validateEnv` command runs checks against the system and reports
any potential problems that may prevent Alluxio from starting properly.

The usage is `validateEnv COMMAND [NAME] [OPTIONS]`
where `COMMAND` can be one of the following values:
* `local`: run all validation tasks on the local machine
* `master`: run master validation tasks on the local machine
* `worker`: run worker validation tasks on the local machine
* `all`: run corresponding validation tasks on all master and worker nodes
* `masters`: run master validation tasks on all master nodes
* `workers`: run worker validation tasks on all worker nodes
* `list`: list all validation tasks

```bash
# Runs all validation tasks on the local machine
$ ./bin/alluxio validateEnv local

# Runs corresponding validation tasks on all master and worker nodes
$ ./bin/alluxio validateEnv all

# Lists all validation tasks
$ ./bin/alluxio validateEnv list
```

For all commands except `list`, `NAME` specifies the leading prefix of any number of tasks.
If `NAME` is not given, all tasks for the given `COMMAND` will run.

```bash
# Only run validation tasks that check your local system resource limits
$ ./bin/alluxio validateEnv ulimit
# Only run the tasks start with "ma", like "master.rpc.port.available" and "master.web.port.available"
$ ./bin/alluxio validateEnv local ma
```

`OPTIONS` can be a list of command line options. Each option has the format
`-<optionName> [optionValue]` For example, `[-hadoopConfDir <arg>]` could set the path to
server-side hadoop configuration directory when running validating tasks.

## File System Operations

```bash
$ ./bin/alluxio fs
Usage: alluxio fs [generic options]
       [cat <path>]
       [checkConsistency [-r] <Alluxio path>]
       ...
```

For `fs` subcommands that take Alluxio URIs as argument (e.g. `ls`, `mkdir`), the argument should
be either a complete Alluxio URI, such as `alluxio://<master-hostname>:<master-port>/<path>`,
or a path without its header, such as `/<path>`, to use the default hostname and port set in the
`conf/allluxio-site.properties`.

>**Wildcard input**
>
>Most of the commands which require path components allow wildcard arguments for ease of use. For
>example:
>
>{% include Command-Line-Interface/rm.md %}
>
>The example command deletes anything in the `data` directory with a prefix of `2014`.
>
>Note that some shells will attempt to glob the input paths, causing strange errors (Note: the
>number 21 could be different and comes from the number of matching files in your local
>filesystem):
>
>{% include Command-Line-Interface/rm-error.md %}
>
>As a workaround, you can disable globbing (depending on the shell type; for example, `set -f`) or by
>escaping wildcards, for example:
>
>{% include Command-Line-Interface/escape.md %}
>
>Note the double escape; this is because the shell script will eventually call a java program
>which should have the final escaped parameters (`cat /\\*`).

### cat

The `cat` command prints the contents of a file in Alluxio to the console.
If you wish to copy the file to your local file system, `copyToLocal` should be used.

For example, when testing a new computation job, `cat` can be used as a quick way to check the output:

{% include Command-Line-Interface/cat.md %}

### checkConsistency

The `checkConsistency` command compares Alluxio and under storage metadata for a given path.
If the path is a directory, the entire subtree will be compared.
The command returns a message listing each inconsistent file or directory.
The system administrator should reconcile the differences of these files at their discretion.
To avoid metadata inconsistencies between Alluxio and under storages,
design your systems to modify files and directories through Alluxio
and avoid directly modifying the under storage.

If the `-r` option is used, the `checkConsistency` command will repair all inconsistent files and
directories under the given path.
If an inconsistent file or directory exists only in under storage, its metadata will be added to Alluxio.
If an inconsistent file exists in Alluxio and its data is fully present in Alluxio,
its metadata will be loaded to Alluxio again.

NOTE: This command requires a read lock on the subtree being checked, meaning writes and updates
to files or directories in the subtree cannot be completed until this command completes.

For example, `checkConsistency` can be used to periodically validate the integrity of the namespace.

{% include Command-Line-Interface/checkConsistency.md %}

### checksum

The `checksum` command outputs the md5 value of a file in Alluxio.

For example, `checksum` can be used to verify the contents of a file stored in Alluxio.

{% include Command-Line-Interface/checksum.md %}

### chgrp

The `chgrp` command changes the group of the file or directory in Alluxio.
Alluxio supports file authorization with Posix file permission.
Group is an authorizable entity in Posix file permissions model.
The file owner or super user can execute this command to change the group of the file or directory.

Adding `-R` option also changes the group of child file and child directory recursively.

For example, `chgrp` can be used as a quick way to change the group of file:

{% include Command-Line-Interface/chgrp.md %}

### chmod

The `chmod` command changes the permission of file or directory in Alluxio.
Currently, octal mode is supported: the numerical format accepts three octal digits
which refer to permissions for the file owner, the group and other users.
Here is number-permission mapping table:

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

Adding `-R` option also changes the permission of child file and child directory recursively.

For example, `chmod` can be used as a quick way to change the permission of file:

{% include Command-Line-Interface/chmod.md %}

### chown

The `chown` command changes the owner of the file or directory in Alluxio.
For security reasons, the ownership of a file can only be altered by a super user.

For example, `chown` can be used as a quick way to change the owner of file:

{% include Command-Line-Interface/chown.md %}

Adding `-R` option also changes the owner of child file and child directory recursively.

### copyFromLocal

The `copyFromLocal` command copies the contents of a file in the local file system into Alluxio.
If the node you run the command from has an Alluxio worker, the data will be available on that worker.
Otherwise, the data will be copied to a random remote node running an Alluxio worker.
If a directory is specified, the directory and all its contents will be copied recursively.

For example, `copyFromLocal` can be used as a quick way to inject data into the system for processing:

{% include Command-Line-Interface/copyFromLocal.md %}

### copyToLocal

The `copyToLocal` command copies a file in Alluxio to the local file system.
If a directory is specified, the directory and all its contents will be copied recursively.

For example, `copyToLocal` can be used as a quick way to download output data
for additional investigation or debugging.

{% include Command-Line-Interface/copyToLocal.md %}

### count

The `count` command outputs the number of files and folders matching a prefix as well as the total
size of the files.
`count` works recursively and accounts for any nested directories and files.
`count` is best utilized when the user has some predefined naming conventions for their files.

For example, if data files are stored by their date, `count` can be used to determine the number of
data files and their total size for any date, month, or year.

{% include Command-Line-Interface/count.md %}

### cp

The `cp` command copies a file or directory in the Alluxio file system
or between the local file system and Alluxio file system.

Scheme `file` indicates the local file system
whereas scheme `alluxio` or no scheme indicates the Alluxio file system.

If the `-R` option is used and the source designates a directory,
`cp` copies the entire subtree at source to the destination.

For example, `cp` can be used to copy files between under storage systems.

{% include Command-Line-Interface/cp.md %}

### du

The `du` command outputs the total size and in Alluxio size of files and folders.

If a directory is specified, it will display the total size and in Alluxio size of all files in this directory.
If the `-s` option is used, it will display the aggregate summary of file lengths being displayed.

By default, `du` prints the size in bytes. If the `-h` option is used, it will print sizes in human readable format (e.g., 1KB 234MB 2GB).

The `--memory` option will print the in memory size as well.

{% include Command-Line-Interface/du.md %}

### fileInfo

The `fileInfo` command is deprecated since Alluxio version 1.5.
Please use `alluxio fs stat <path>` command instead.

The `fileInfo` command dumps the FileInfo representation of a file to the console.
It is primarily intended to assist power users in debugging their system.
Generally viewing the file info in the UI is much easier to understand.

For example, `fileInfo` can be used to debug the block locations of a file.
This is useful when trying to achieve locality for compute workloads.

{% include Command-Line-Interface/fileInfo.md %}

### free

The `free` command sends a request to the master to evict all blocks of a file from the Alluxio workers.
If the argument to `free` is a directory, it will recursively `free` all files.
This request is not guaranteed to take effect immediately,
as readers may be currently using the blocks of the file.
`free` will return immediately after the request is acknowledged by the master.
Note that files must be already persisted in under storage before being freed or the `free` command will fail.
Any pinned files cannot be freed unless `-f` option is specified.
The `free` command does not delete any data from the under storage system,
only removing the blocks of those files in Alluxio space to reclaim space.
Metadata is not affected by this operation; a freed file will still show up if an `ls` command is run.

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

For example, `getUsedBytes` can be used to monitor the health of the cluster.

{% include Command-Line-Interface/getUsedBytes.md %}

### head

The `head` command prints the first 1 KB of data in a file to the console.

Using the `-c [bytes]` option will print the first `n` bytes of data to the console.

```bash
$ ./bin/alluxio fs head -c 2048 /output/part-00000
```

### help

The `help` command prints the help message for a given `fs` subcommand.
If the given command does not exist, it prints help messages for all supported subcommands.

Examples:

```bash
# Print all subcommands
$ ./bin/alluxio fs help
#
# Print help message for ls
$ ./bin/alluxio fs help ls
```

### leader

The `leader` command prints the current Alluxio leading master hostname.

{% include Command-Line-Interface/leader.md %}

### load

The `load` command moves data from the under storage system into Alluxio storage.
If there is a Alluxio worker on the machine this command is run from, the data will be loaded to that worker.
Otherwise, a random worker will be selected to serve the data.

If the data is already loaded into Alluxio, load is a no-op unless the `--local flag` is used.
The `--local` flag forces the data to be loaded to a local worker
even if the data is already available on a remote worker.
If `load` is run on a directory, files in the directory will be recursively loaded.

For example, `load` can be used to prefetch data for analytics jobs.

{% include Command-Line-Interface/load.md %}

### loadMetadata

The `loadMetadata` command is deprecated since Alluxio version 1.1.
Please use `alluxio fs ls <path>` command instead.

The `loadMetadata` command queries the under storage system for any file or directory matching the
given path and creates a mirror of the file in Alluxio backed by that file.
Only the metadata, such as the file name and size, are loaded this way and no data transfer occurs.

For example, `loadMetadata` can be used when other systems output to the under storage directly
and the application running on Alluxio needs to use the output of those systems.

{% include Command-Line-Interface/loadMetadata.md %}

### location

The `location` command returns the addresses of all the Alluxio workers which contain blocks
belonging to the given file.

For example, `location` can be used to debug data locality when running jobs using a compute framework.

{% include Command-Line-Interface/location.md %}

### ls

The `ls` command lists all the immediate children in a directory and displays the file size, last
modification time, and in memory status of the files.
Using `ls` on a file will only display the information for that specific file.

The `ls` command will also load the metadata for any file or immediate children of a directory
from the under storage system to Alluxio namespace if it does not exist in Alluxio.
`ls` queries the under storage system for any file or directory matching the given path
and creates a mirror of the file in Alluxio backed by that file.
Only the metadata, such as the file name and size, are loaded this way and no data transfer occurs.

Options:

* `-d` option lists the directories as plain files. For example, `ls -d /` shows the attributes of root directory.
* `-f` option forces loading metadata for immediate children in a directory.
By default, it loads metadata only at the first time at which a directory is listed.
* `-h` option displays file sizes in human-readable formats.
* `-p` option lists all pinned files.
* `-R` option also recursively lists child directories, displaying the entire subtree starting from the input path.
* `--sort` sorts the result by the given option. Possible values are size, creationTime, inMemoryPercentage, lastModificationTime, and path.
* `-r` reverses the sorting order.

For example, `ls` can be used to browse the file system.

{% include Command-Line-Interface/ls.md %}

### masterInfo

The `masterInfo` command prints information regarding master fault tolerance such as leader address,
list of master addresses, and the configured Zookeeper address.
If Alluxio is running in single master mode, `masterInfo` prints the master address.
If Alluxio is running in fault tolerance mode, the leader address, list of master addresses
and the configured Zookeeper address is printed.

For example, `masterInfo` can be used to print information regarding master fault tolerance.

{% include Command-Line-Interface/masterInfo.md %}

### mkdir

The `mkdir` command creates a new directory in Alluxio space.
It is recursive and will create any nonexistent parent directories.
Note that the created directory will not be created in the under storage system
until a file in the directory is persisted to the underlying storage.
Using `mkdir` on an invalid or existing path will fail.

For example, `mkdir` can be used by an admin to set up the basic folder structures.

{% include Command-Line-Interface/mkdir.md %}

### mount

The `mount` command links an under storage path to an Alluxio path,
where files and folders created in Alluxio space under the path will be backed
by a corresponding file or folder in the under storage path.
For more details, see [Unified Namespace]({{ '/en/advanced/Namespace-Management.html' | relativize_url }}).

Options:

* `--readonly` option sets the mount point to be readonly in Alluxio
* `--option <key>=<val>` option passes an property to this mount point, such as S3 credentials
* `--shared` option sets the mount point to be shared with all Alluxio users.

Note that `--readonly` mounts are useful to prevent accidental write operations.
If multiple Alluxio satellite clusters mount a remote storage cluster which serves as the central source of truth,
`--readonly` option could help prevent any write operations on the satellite cluster from wiping out the remote storage.

For example, `mount` can be used to make data in another storage system available in Alluxio.

{% include Command-Line-Interface/mount.md %}

### mv

The `mv` command moves a file or directory to another path in Alluxio.
The destination path must not exist or be a directory.
If it is a directory, the file or directory will be placed as a child of the directory.
`mv` is purely a metadata operation and does not affect the data blocks of the file.
`mv` cannot be done between mount points of different under storage systems.

For example, `mv` can be used to re-organize your files.

{% include Command-Line-Interface/mv.md %}

### persist

The `persist` command persists data in Alluxio storage into the under storage system.
This is a server side data operation and will take time depending on how large the file is.
After persist is complete, the file in Alluxio will be backed by the file in the under storage,
and will still be available if the Alluxio blocks are evicted or otherwise lost.

If you are persisting multiple files, you can use the `--parallelism <#>` option to submit `#` of
persist commands in parallel. For example, if your folder has 10,000 files, persisting with a
parallelism factor of 10 will persist 10 files at a time until all 10,000 files are persisted.

For example, `persist` can be used after filtering a series of temporary files for the ones containing useful data.

{% include Command-Line-Interface/persist.md %}

### pin

The `pin` command marks a file or folder as pinned in Alluxio.
This is a metadata operation and will not cause any data to be loaded into Alluxio.
If a file is pinned, any blocks belonging to the file will never be evicted from an Alluxio worker.
If there are too many pinned files, Alluxio workers may run low on storage space,
preventing other files from being cached.

For example, `pin` can be used to manually ensure performance
if the administrator understands the workloads well.

{% include Command-Line-Interface/pin.md %}

### rm

The `rm` command removes a file from Alluxio space and the under storage system.
The file will be unavailable immediately after this command returns,
but the actual data may be deleted a while later.

* Adding `-R` option deletes all contents of the directory and the directory itself.
* Adding `-U` option skips the check for whether the UFS contents being deleted are in-sync with Alluxio
before attempting to delete persisted directories.
* Adding `--alluxioOnly` option removes data and metadata from Alluxio space only.
The under storage system will not be affected.

{% include Command-Line-Interface/rm2.md %}

### setfacl

The `setfacl` command modifies the access control list associated with a specified file or directory.

The`-R` option applies operations to all files and directories recursively.
The `-m` option modifies the ACL by adding/overwriting new entries.
The `-x` option removes specific ACL entries.
The `-b` option removes all ACL entries, except for the base entries.
The `-k` option removes all the default ACL entries.

For example, `setfacl` can be used to give read and execute permissions to a user named `testuser`.

```bash
$ ./bin/alluxio fs setfacl -m "user:testuser:r-x" /testdir/testfile
```

### setTtl

The `setTtl` command sets the time-to-live of a file or a directory, in milliseconds.
If set TTL is run on a directory, the same TTL attributes is set on all its children.
If a directory's TTL expires, all its children will also expire.

Action parameter `--action` will indicate the action to perform once the file or directory expires.
The default action, `delete`, deletes the file or directory from both Alluxio and the under storage system,
whereas the action `free` frees the file from Alluxio even if pinned.

For example, `setTtl` with action `delete` cleans up files the administrator knows are unnecessary after a period of time,
or with action `free` just remove the contents from Alluxio to make room for more space in Alluxio.

{% include Command-Line-Interface/setTtl.md %}

### stat

The `stat` command dumps the FileInfo representation of a file or a directory to the console.
It is primarily intended to assist power users in debugging their system.
Generally viewing the file info in the UI will be easier to understand.

One can specify `-f <arg>` to display info in given format:
* `%N`: name of the file
* `%z`: size of file in bytes
* `%u`: owner
* `%g`: group name of owner
* `%y` or `%Y`: modification time, where `%y` shows the UTC date in the form `yyyy-MM-dd HH:mm:ss`
 and `%Y` shows the number of milliseconds since January 1, 1970 UTC
* `%b`: Number of blocks allocated for file

For example, `stat` can be used to debug the block locations of a file.
This is useful when trying to achieve locality for compute workloads.

{% include Command-Line-Interface/stat.md %}

### tail

The `tail` command outputs the last 1 KB of data in a file to the console.
Using the `-c [bytes]` option will print the last `n` bytes of data to the console.

For example, `tail` can be used to verify the output of a job is in the expected format
or contains expected values.

{% include Command-Line-Interface/tail.md %}

### test

The `test` command tests a property of a path,
returning `0` if the property is true or `1` otherwise.

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

The `touch` command creates a 0-byte file.
Files created with `touch` cannot be overwritten and are mostly useful as flags.

For example, `touch` can be used to create a file signifying the completion of analysis on a directory.

{% include Command-Line-Interface/touch.md %}

### unmount

The `unmount` command disassociates an Alluxio path with an under storage directory.
Alluxio metadata for the mount point is removed along with any data blocks,
but the under storage system will retain all metadata and data.
See [Unified Namespace]({{ '/en/advanced/Namespace-Management.html' | relativize_url }}) for more details.

For example, `unmount` can be used to remove an under storage system when the users no longer need
data from that system.

{% include Command-Line-Interface/unmount.md %}

### unpin

The `unpin` command unmarks a file or directory in Alluxio as pinned.
This is a metadata operation and will not evict or delete any data blocks.
Once a file is unpinned, its data blocks can be evicted
from the various Alluxio workers containing the block.

For example, `unpin` can be used when the administrator knows there is a change in the data access pattern.

{% include Command-Line-Interface/unpin.md %}

### unsetTtl

The `unsetTtl` command will remove the TTL attributes of a file or directory in Alluxio.
This is a metadata operation and will not evict or store blocks in Alluxio.
The TTL of a file can later be reset with `setTtl`.

For example, `unsetTtl` can be used if a regularly managed file requires manual management.

{% include Command-Line-Interface/unsetTtl.md %}
