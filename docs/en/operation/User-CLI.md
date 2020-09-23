---
layout: global
title: User Command Line Interface
nickname: User CLI
group: Operations
priority: 1
---

* Table of Contents
{:toc}

Alluxio's command line interface provides users with basic file system operations. You can invoke
the following command line utility to get all the subcommands:

```console
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

### extensions

The `extensions` command is for managing UFS extensions to Alluxio. For additional information, refer
to the [main page]({{ '/en/ufs/Ufs-Extensions.html' | relativize_url }}).

### format

The `format` command formats the Alluxio master and all its workers.

If `-s` specified, only format if under storage is local and does not already exist

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio,
including cached data and any metadata information.
Data in under storage will not be changed.

Warning: `format` is required when you run Alluxio for the first time.
`format` should only be called while the cluster is not running.

```console
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


```console
$ ./bin/alluxio formatMaster
```

### formatWorker

The `formatWorker` command formats the Alluxio worker.

An Alluxio worker caches files and objects.

`formatWorker` deletes all the cached data stored in this worker node.
Data in under storage will not be changed.

Warning: `formatWorker` should only be called while the cluster is not running.

```console
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

```console
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

### fs

See [File System Operations](#file-system-operations).

### fsadmin

The `fsadmin` command is meant for administrators of the Alluxio cluster.
It provides added tools for diagnostics and troubleshooting.
For more information see the [main page]({{ '/en/operation/Admin-CLI.html' | relativize_url }}).

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

```console
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

### job

The `job` command is a tool for interacting with the job service.

The usage is `job [generic options]`
where `[generic options]` can be one of the following values:
* `leader`: Prints the hostname of the job master service leader.
* `ls`: Prints the IDs of the most recent jobs, running and finished, in the history up to the capacity set in `alluxio.job.master.job.capacity`.
* `stat [-v] <id>`:Displays the status info for the specific job. Use -v flag to display the status of every task.

```console
# Prints the hostname of the job master service leader.
$ ./bin/alluxio job leader

# Prints the IDs, job names, and completion status of the most recently created jobs.
$ ./bin/alluxio job ls
1576539334518 Load COMPLETED
1576539334519 Load CREATED
1576539334520 Load CREATED
1576539334521 Load CREATED
1576539334522 Load CREATED
1576539334523 Load CREATED
1576539334524 Load CREATED
1576539334525 Load CREATED
1576539334526 Load CREATED

# Displays the status info for the specific job.
$ bin/alluxio job stat -v 1579102592778
ID: 1579102592778
Name: Migrate
Description: MigrateConfig{source=/test, destination=/test2, writeType=ASYNC_THROUGH, overwrite=true, delet...
Status: CANCELED
Task 0
Worker: localhost
Status: CANCELED
Task 1
Worker: localhost
Status: CANCELED
Task 2
Worker: localhost
Status: CANCELED
...
...

# Cancels the job asynchronously based on a specific job.
$ bin/alluxio job cancel 1579102592778

$ bin/alluxio job stat 1579102592778 | grep "Status"
Status: CANCELED
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

```console
$ ./bin/alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext \
  --target=master,192.168.100.100:30000 --level=DEBUG
```

And the following command returns the log level of the class `alluxio.heartbeat.HeartbeatContext` among all the workers:
```console
$ ./bin/alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext \
  --target=workers
```

### runClass

The `runClass` command runs the main method of an Alluxio class.

For example, to run the multi-mount demo:
```console
$ ./bin/alluxio runClass alluxio.examples.MultiMount <HDFS_URL>
```

### runTests

The `runTests` command runs end-to-end tests on an Alluxio cluster to provide a comprehensive sanity check.

```console
$ ./bin/alluxio runTests
```

### runJournalCrashTest

The `runJournalCrashTest` simulates a failover to test recovery from the journal.
Note that this command will stop any Alluxio services running on the machine.

### runHmsTests

The `runHmsTests` aims to validate the configuration, connectivity, and permissions of an existing hive metastore 
which is an important component in compute workflows with Alluxio.

* `-h` provides detailed guidance.
* `-m <hive_metastore_uris>` (required) the full hive metastore uris to connect to an existing hive metastore.
* `-d <databse_name>` the database to run tests against. Use `default` database if not provided.
* `-t` tables to run tests against. Run tests against five out of all tables in the given database if not provided.
* `-st` socket timeout of hive metastore client in minutes.

```
$ ./bin/alluxio runHmsTests -m thrift://<hms_host>:<hms_port> -d tpcds -t store_sales,web_sales 
```

This tool is suggested to run from compute application environments and checks
* if the given hive metastore uris are valid
* if the hive metastore client connection can be established with the target server
* if hive metastore client operations can be run against the given database and tables

### runHdfsMountTests

The `runHdfsMountTests` command aims to validate the configuration, connectivity and permissions of an HDFS path.
It validates various aspects for connecting to HDFS with the given Alluxio configurations and identifies issues 
before the path is mounted to Alluxio.
This tool will validate a few criteria and return the feedback.
If a test failed, advice will be given correspondingly on how the user can rectify the setup.

Options:

* `--help` provides detailed guidance.
* `--readonly` specifies the mount point should be readonly in Alluxio.
* `--shared` specifies the mount point should be accessible for all Alluxio users.
* `--option <key>=<val>` passes an property to this mount point.
* `<hdfs-path>` specifies the HDFS path you want to validate (then mount to Alluxio)

The arguments to this command should be consistent to what you give to the 
[Mount command](#mount), in order to validate the setup for the mount.

```console
# If this is your mount command
$ bin/alluxio fs mount --readonly --option alluxio.underfs.version=2.7 \
  --option alluxio.underfs.hdfs.configuration=/etc/hadoop/core-site.xml:/etc/hadoop/hdfs-site.xml \
  <alluxio-path> hdfs://<hdfs-path>

# Pass the same options to runHdfsMountTests
$ bin/alluxio runHdfsMountTests --readonly --option alluxio.underfs.version=2.7 \
  --option alluxio.underfs.hdfs.configuration=/etc/hadoop/core-site.xml:/etc/hadoop/hdfs-site.xml \
  hdfs://<hdfs-path>
```

> Note: This command DOES NOT mount the HDFS path to Alluxio.

### runUfsIOTest

The `runUfsIOTest` command measures the read/write IO throughput from Alluxio cluster to the target HDFS.

Options:

* `-h, --help` provides detailed guidance.
* `--path <hdfs-path>` specifies the path to write/read temporary data in. This is a compulsory field.
* `--io-size <io-size>` specifies the amount of data each thread writes/reads. It defaults to "4G".
* `--threads <thread-num>` specifies the number of threads to concurrently use on each worker. It defaults to 4.
* `--cluster` specifies the benchmark is run in the Alluxio cluster. If not specified, this benchmark will run locally.
* `--cluster-limit <worker-num>` specifies how many Alluxio workers to run the benchmark concurrently.
       If `>0`, it will only run on that number of workers. 
       If `0`, it will run on all available cluster workers. 
       If `<0`, will run on the workers from the end of the worker list. 
       This flag is only used if `--cluster` is enabled.
       This default to 0.
* `--java-opt <java-opt>` The java options to add to the command line to for the task. 
       This can be repeated. The options must be quoted and prefixed with a space. 
       For example: `--java-opt " -Xmx4g" --java-opt " -Xms2g"`.

Examples:
```console
# This runs the I/O benchmark to HDFS in your process locally
$ bin/alluxio runUfsIOTest --path hdfs://<hdfs-address>

# This invokes the I/O benchmark to HDFS in the Alluxio cluster
# 1 worker will be used. 4 threads will be created, each writing then reading 4G of data 
$ bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --cluster --cluster-limit 1

# This invokes the I/O benchmark to HDFS in the Alluxio cluster
# 2 workers will be used
# 2 threads will be created on each worker
# Each thread is writing then reading 512m of data
$ bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --cluster --cluster-limit 2 \
  --io-size 512m --threads 2
```

### runUfsTests

The `runUfsTests` aims to test the integration between Alluxio and the given UFS. UFS tests
validate the semantics Alluxio expects of the UFS.

`--help` provides detailed guidance.
`--path <ufs_path>` (required) the full UFS path to run tests against.

The usage of this command includes:
* Test if the given UFS credentials are valid before mounting the UFS to an Alluxio cluster.
* If the given UFS is S3, this test can also be used as a S3 compatibility test to test if the target under filesystem can
  fulfill the minimum S3 compatibility requirements in order to work well with Alluxio.
* Validate the contract between Alluxio and the given UFS. This is primarily intended for Alluxio developers.
  Developers are required to add test coverage for changes to an Alluxio UFS module and run those tests to validate.

```console
# Run tests against local UFS
$ ./bin/alluxio runUfsTests --path /local/underfs/path

# Run tests against S3
$ ./bin/alluxio runUfsTests --path s3://<s3_bucket_name> \
  -Daws.accessKeyId=<access_key> -Daws.secretKey=<secret_key> \
  -Dalluxio.underfs.s3.endpoint=<endpoint_url> -Dalluxio.underfs.s3.disable.dns.buckets=true
```

### readJournal

The `readJournal` command parses the current journal and outputs a human readable version to the local folder.
Note this command may take a while depending on the size of the journal.

### upgradeJournal

The `upgradeJournal` command upgrades an Alluxio journal version 0 (Alluxio version < 1.5.0)
to an Alluxio journal version 1 (Alluxio version >= 1.5.0).

`-journalDirectoryV0 <arg>` will provide the v0 journal persisted location.\
It is assumed to be the same as the v1 journal directory if not set.

```console
$ ./bin/alluxio upgradeJournal
```

### killAll

The `killAll` command kills all processes containing the specified word.
Note this kills non-Alluxio processes as well.

### copyDir

The `copyDir` command copies the directory at `PATH` to all worker nodes listed in `conf/workers`.

```console
$ ./bin/alluxio copyDir conf/alluxio-site.properties
```

### clearCache

The `clearCache` command drops the OS buffer cache.

### docGen

The `docGen` command autogenerates documentation based on the current source code.

### table

See [Table Operations](#table-operations).

### version

The `version` command prints Alluxio version.

```console
$ ./bin/alluxio version
```

### validateConf

The `validateConf` command validates the local Alluxio configuration files, checking for common misconfigurations.

```console
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

```console
# Runs all validation tasks on the local machine
$ ./bin/alluxio validateEnv local

# Runs corresponding validation tasks on all master and worker nodes
$ ./bin/alluxio validateEnv all

# Lists all validation tasks
$ ./bin/alluxio validateEnv list
```

For all commands except `list`, `NAME` specifies the leading prefix of any number of tasks.
If `NAME` is not given, all tasks for the given `COMMAND` will run.

```console
# Only run validation tasks that check your local system resource limits
$ ./bin/alluxio validateEnv ulimit
# Only run the tasks start with "ma", like "master.rpc.port.available" and "master.web.port.available"
$ ./bin/alluxio validateEnv local ma
```

`OPTIONS` can be a list of command line options. Each option has the format
`-<optionName> [optionValue]` For example, `[-hadoopConfDir <arg>]` could set the path to
server-side hadoop configuration directory when running validating tasks.

### collectInfo

The `collectInfo` command collects information to troubleshoot an Alluxio cluster.
For more information see the [collectInfo command page]({{ '/en/operation/Troubleshooting.html#alluxio-collectinfo-command' | relativize_url }}).

## File System Operations

```
./bin/alluxio fs
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
>```console
>$ ./bin/alluxio fs rm '/data/2014*'
>```
>
>The example command deletes anything in the `data` directory with a prefix of `2014`.
>
>Note that some shells will attempt to glob the input paths, causing strange errors (Note: the
>number 21 could be different and comes from the number of matching files in your local
>filesystem):
>
>```
>rm takes 1 arguments,  not 21
>```
>
>As a workaround, you can disable globbing (depending on the shell type; for example, `set -f`) or by
>escaping wildcards, for example:
>
>```console
>$ ./bin/alluxio fs cat /\\*
>```
>
>Note the double escape; this is because the shell script will eventually call a java program
>which should have the final escaped parameters (`cat /\\*`).

### cat

The `cat` command prints the contents of a file in Alluxio to the console.
If you wish to copy the file to your local file system, `copyToLocal` should be used.

For example, when testing a new computation job, `cat` can be used as a quick way to check the output:

```console
$ ./bin/alluxio fs cat /output/part-00000
```

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

```console
# List each inconsistent file or directory
$ ./bin/alluxio fs checkConsistency /

# Repair the inconsistent files or directories
$ ./bin/alluxio fs checkConsistency -r /
```

### checksum

The `checksum` command outputs the md5 value of a file in Alluxio.

For example, `checksum` can be used to verify the contents of a file stored in Alluxio.

```console
$ ./bin/alluxio fs checksum /LICENSE
md5sum: bf0513403ff54711966f39b058e059a3
md5 LICENSE
MD5 (LICENSE) = bf0513403ff54711966f39b058e059a3
```

### chgrp

The `chgrp` command changes the group of the file or directory in Alluxio.
Alluxio supports file authorization with Posix file permission.
Group is an authorizable entity in Posix file permissions model.
The file owner or super user can execute this command to change the group of the file or directory.

Adding `-R` option also changes the group of child file and child directory recursively.

For example, `chgrp` can be used as a quick way to change the group of file:

```console
$ ./bin/alluxio fs chgrp alluxio-group-new /input/file1
```

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

```console
$ ./bin/alluxio fs chmod 755 /input/file1
```

### chown

The `chown` command changes the owner of the file or directory in Alluxio.
For security reasons, the ownership of a file can only be altered by a super user.

For example, `chown` can be used as a quick way to change the owner of file:

```console
$ ./bin/alluxio fs chown alluxio-user /input/file1
$ ./bin/alluxio fs chown alluxio-user:alluxio-group /input/file2
```

Adding `-R` option also changes the owner of child file and child directory recursively.

### copyFromLocal

The `copyFromLocal` command copies the contents of a file in the local file system into Alluxio.
If the node you run the command from has an Alluxio worker, the data will be available on that worker.
Otherwise, the data will be copied to a random remote node running an Alluxio worker.
If a directory is specified, the directory and all its contents will be copied recursively.

For example, `copyFromLocal` can be used as a quick way to inject data into the system for processing:

```console
$ ./bin/alluxio fs copyFromLocal /local/data /input
```

### copyToLocal

The `copyToLocal` command copies a file in Alluxio to the local file system.
If a directory is specified, the directory and all its contents will be copied recursively.

For example, `copyToLocal` can be used as a quick way to download output data
for additional investigation or debugging.

```console
$ ./bin/alluxio fs copyToLocal /output/part-00000 part-00000
$ wc -l part-00000
```

### count

The `count` command outputs the number of files and folders matching a prefix as well as the total
size of the files.
`count` works recursively and accounts for any nested directories and files.
`count` is best utilized when the user has some predefined naming conventions for their files.

For example, if data files are stored by their date, `count` can be used to determine the number of
data files and their total size for any date, month, or year.

```console
$ ./bin/alluxio fs count /data/2014
```

### cp

The `cp` command copies a file or directory in the Alluxio file system
or between the local file system and Alluxio file system.

Scheme `file` indicates the local file system
whereas scheme `alluxio` or no scheme indicates the Alluxio file system.

If the `-R` option is used and the source designates a directory,
`cp` copies the entire subtree at source to the destination.

For example, `cp` can be used to copy files between under storage systems.

```console
$ ./bin/alluxio fs cp /hdfs/file1 /s3/
```

### distributedCp

The `distributedCp` command copies a file or directory in the Alluxio file system distributed across workers
using the job service.

If the source designates a directory, `distributedCp` copies the entire subtree at source to the destination.

```console
$ ./bin/alluxio fs distributedCp /data/1023 /data/1024
```

### distributedLoad

The `distributedLoad` command loads a file or directory from the under storage system into Alluxio storage distributed 
across workers using the job service. The job is a no-op if the file is already loaded into Alluxio.

If `distributedLoad` is run on a directory, files in the directory will be recursively loaded and each file will be loaded
on a random worker.
The `--replication` flag can be used to load the data into multiple workers.

```console
$ ./bin/alluxio fs distributedLoad --replication 2 /data/today
```

### distributedMv

The `distributedMv` command moves a file or directory in the Alluxio file system distributed across workers
using the job service.

If the source designates a directory, `distributedMv` moves the entire subtree at source to the destination.

```console
$ ./bin/alluxio fs distributedMv /data/1023 /data/1024
```

### du

The `du` command outputs the total size and in Alluxio size of files and folders.

If a directory is specified, it will display the total size and in Alluxio size of all files in this directory.
If the `-s` option is used, it will display the aggregate summary of file lengths being displayed.

By default, `du` prints the size in bytes. If the `-h` option is used, it will print sizes in human readable format (e.g., 1KB 234MB 2GB).

The `--memory` option will print the in memory size as well.

```console
# Shows the size information of all the files in root directory
$ ./bin/alluxio fs du /
File Size     In Alluxio       Path
1337          0 (0%)           /alluxio-site.properties
4352          4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           /testDir/LICENSE
2970          2970 (100%)      /testDir/README.md

# Shows the in memory size information
$ ./bin/alluxio fs du --memory /
File Size     In Alluxio       In Memory        Path
1337          0 (0%)           0 (0%)           /alluxio-site.properties
4352          4352 (100%)      4352 (100%)      /testFolder/NOTICE
26847         0 (0%)           0 (0%)           /testDir/LICENSE
2970          2970 (100%)      2970 (100%)      /testDir/README.md

# Shows the aggregate size information in human-readable format
./bin/alluxio fs du -h -s /
File Size     In Alluxio       In Memory        Path
34.67KB       7.15KB (20%)     7.15KB (20%)     /

# Can be used to detect which folders are taking up the most space
./bin/alluxio fs du -h -s /\\*
File Size     In Alluxio       Path
1337B         0B (0%)          /alluxio-site.properties
29.12KB       2970B (9%)       /testDir
4352B         4352B (100%)     /testFolder
```

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

```console
$ ./bin/alluxio fs free /unused/data
```

### getCapacityBytes

The `getCapacityBytes` command returns the maximum number of bytes Alluxio is configured to store.

For example, `getCapacityBytes` can be used to verify if your cluster is set up as expected.

```console
$ ./bin/alluxio fs getCapacityBytes
```

### getfacl

The `getfacl` command returns the ACL entries for a specified file or directory.

For example, `getfacl` can be used to verify that an ACL is changed successfully after a call to `setfacl`.

```console
$ ./bin/alluxio fs getfacl /testdir/testfile
```

### getUsedBytes

The `getUsedBytes` command returns the number of used bytes in Alluxio.

For example, `getUsedBytes` can be used to monitor the health of the cluster.

```console
$ ./bin/alluxio fs getUsedBytes
```

### head

The `head` command prints the first 1 KB of data in a file to the console.

Using the `-c [bytes]` option will print the first `n` bytes of data to the console.

```console
$ ./bin/alluxio fs head -c 2048 /output/part-00000
```

### help

The `help` command prints the help message for a given `fs` subcommand.
If the given command does not exist, it prints help messages for all supported subcommands.

Examples:

```console
# Print all subcommands
$ ./bin/alluxio fs help

# Print help message for ls
$ ./bin/alluxio fs help ls
```

### leader

The `leader` command prints the current Alluxio leading master hostname.

```console
$ ./bin/alluxio fs leader
```

### load

The `load` command moves data from the under storage system into Alluxio storage.
If there is a Alluxio worker on the machine this command is run from, the data will be loaded to that worker.
Otherwise, a random worker will be selected to serve the data.

If the data is already loaded into Alluxio, load is a no-op unless the `--local flag` is used.
The `--local` flag forces the data to be loaded to a local worker
even if the data is already available on a remote worker.
If `load` is run on a directory, files in the directory will be recursively loaded.

For example, `load` can be used to prefetch data for analytics jobs.

```console
$ ./bin/alluxio fs load /data/today
```

### location

The `location` command returns the addresses of all the Alluxio workers which contain blocks
belonging to the given file.

For example, `location` can be used to debug data locality when running jobs using a compute framework.

```console
$ ./bin/alluxio fs location /data/2015/logs-1.txt
```

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
* `--sort` sorts the result by the given option. Possible values are size, creationTime, inMemoryPercentage, lastModificationTime, lastAccessTime and path.
* `--timestamp` display the timestamp of the given option. Possible values are creationTime, lastModificationTime, and lastAccessTime.
The default option is lastModificationTime.
* `-r` reverses the sorting order.

For example, `ls` can be used to browse the file system.

```console
$ ./bin/alluxio fs mount /s3/data s3://data-bucket/
# Loads metadata for all immediate children of /s3/data and lists them.
$ ./bin/alluxio fs ls /s3/data/

# Forces loading metadata.
$ aws s3 cp /tmp/somedata s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data

# Files are not removed from Alluxio if they are removed from the UFS (s3 here) only.
$ aws s3 rm s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
```

### masterInfo

The `masterInfo` command prints information regarding master fault tolerance such as leader address,
list of master addresses, and the configured Zookeeper address.
If Alluxio is running in single master mode, `masterInfo` prints the master address.
If Alluxio is running in fault tolerance mode, the leader address, list of master addresses
and the configured Zookeeper address is printed.

For example, `masterInfo` can be used to print information regarding master fault tolerance.

```console
$ ./bin/alluxio fs masterInfo
```

### mkdir

The `mkdir` command creates a new directory in Alluxio space.
It is recursive and will create any nonexistent parent directories.
Note that the created directory will not be created in the under storage system
until a file in the directory is persisted to the underlying storage.
Using `mkdir` on an invalid or existing path will fail.

For example, `mkdir` can be used by an admin to set up the basic folder structures.

```console
$ ./bin/alluxio fs mkdir /users
$ ./bin/alluxio fs mkdir /users/Alice
$ ./bin/alluxio fs mkdir /users/Bob
```

### mount

The `mount` command links an under storage path to an Alluxio path,
where files and folders created in Alluxio space under the path will be backed
by a corresponding file or folder in the under storage path.
For more details, see [Unified Namespace]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }}).

Options:

* `--option <key>=<val>` option passes an property to this mount point, such as S3 credentials
* `--readonly` option sets the mount point to be readonly in Alluxio
* `--shared` option sets the permission bits of the mount point to be accessible for all Alluxio users

Note that `--readonly` mounts are useful to prevent accidental write operations.
If multiple Alluxio satellite clusters mount a remote storage cluster which serves as the central source of truth,
`--readonly` option could help prevent any write operations on the satellite cluster from wiping out the remote storage.

For example, `mount` can be used to make data in another storage system available in Alluxio.

```console
$ ./bin/alluxio fs mount /mnt/hdfs hdfs://host1:9000/data/
$ ./bin/alluxio fs mount --shared --readonly /mnt/hdfs2 hdfs://host2:9000/data/
$ ./bin/alluxio fs mount \
  --option aws.accessKeyId=<accessKeyId> \
  --option aws.secretKey=<secretKey> \
  /mnt/s3 s3://data-bucket/
```

### mv

The `mv` command moves a file or directory to another path in Alluxio.
The destination path must not exist or be a directory.
If it is a directory, the file or directory will be placed as a child of the directory.
`mv` is purely a metadata operation and does not affect the data blocks of the file.
`mv` cannot be done between mount points of different under storage systems.

For example, `mv` can be used to re-organize your files.

```console
$ ./bin/alluxio fs mv /data/2014 /data/archives/2014
```

### persist

The `persist` command persists data in Alluxio storage into the under storage system.
This is a server side data operation and will take time depending on how large the file is.
After persist is complete, the file in Alluxio will be backed by the file in the under storage,
and will still be available if the Alluxio blocks are evicted or otherwise lost.

If you are persisting multiple files, you can use the `--parallelism <#>` option to submit `#` of
persist commands in parallel. For example, if your folder has 10,000 files, persisting with a
parallelism factor of 10 will persist 10 files at a time until all 10,000 files are persisted.

For example, `persist` can be used after filtering a series of temporary files for the ones containing useful data.

```console
$ ./bin/alluxio fs persist /tmp/experimental-logs-2.txt
```

### pin

The `pin` command marks a file or folder as pinned in Alluxio.
This is a metadata operation and will not cause any data to be loaded into Alluxio.
If a file is pinned, any blocks belonging to the file will never be evicted from an Alluxio worker.
If there are too many pinned files, Alluxio workers may run low on storage space,
preventing other files from being cached.

For example, `pin` can be used to manually ensure performance
if the administrator understands the workloads well.

```console
$ ./bin/alluxio fs pin /data/today
```

### rm

The `rm` command removes a file from Alluxio space and the under storage system.
The file will be unavailable immediately after this command returns,
but the actual data may be deleted a while later.

* Adding `-R` option deletes all contents of the directory and the directory itself.
* Adding `-U` option skips the check for whether the UFS contents being deleted are in-sync with Alluxio
before attempting to delete persisted directories.
* Adding `--alluxioOnly` option removes data and metadata from Alluxio space only.
The under storage system will not be affected.

```console
# Remove a file from Alluxio space and the under storage system
$ ./bin/alluxio fs rm /tmp/unused-file
# Remove a file from Alluxio space only
$ ./bin/alluxio fs rm --alluxioOnly /tmp/unused-file2
```

### setfacl

The `setfacl` command modifies the access control list associated with a specified file or directory.

The`-R` option applies operations to all files and directories recursively.
The `-m` option modifies the ACL by adding/overwriting new entries.
The `-x` option removes specific ACL entries.
The `-b` option removes all ACL entries, except for the base entries.
The `-k` option removes all the default ACL entries.

For example, `setfacl` can be used to give read and execute permissions to a user named `testuser`.

```console
$ ./bin/alluxio fs setfacl -m "user:testuser:r-x" /testdir/testfile
```

### setReplication

The `setReplication` command sets the max and/or min replication level of a file or all files under
a directory recursively. This is a metadata operation and will not cause any replication to be
created or removed immediately. The replication level of the target file or directory will be
changed automatically in background. This command takes an argument of `--min` to specify the
minimal replication level and `--max` for the maximal replication. Specify -1 as the argument of
`--max` option to indicate no limit of the maximum number of replicas. If the specified path is a
directory and `-R` is specified, it will recursively set all files in this directory.

For example, `setReplication` can be used to ensure the replication level of a file has at least
one copy and at most three copies in Alluxio:

```console
$ ./bin/alluxio fs setReplication --max 3 --min 1 /foo
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

```console
# After 1 day, delete the file in Alluxio and UFS
$ ./bin/alluxio fs setTtl /data/good-for-one-day 86400000
# After 1 day, free the file from Alluxio
$ ./bin/alluxio fs setTtl --action free /data/good-for-one-day 86400000
```

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

```console
# Displays file's stat
$ ./bin/alluxio fs stat /data/2015/logs-1.txt

# Displays directory's stat
$ ./bin/alluxio fs stat /data/2015

# Displays the size of file
$ ./bin/alluxio fs stat -f %z /data/2015/logs-1.txt
```

### tail

The `tail` command outputs the last 1 KB of data in a file to the console.
Using the `-c [bytes]` option will print the last `n` bytes of data to the console.

For example, `tail` can be used to verify the output of a job is in the expected format
or contains expected values.

```console
$ ./bin/alluxio fs tail /output/part-00000
```

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

```console
$ ./bin/alluxio fs test -d /someDir
$ echo $?
```

### touch

The `touch` command creates a 0-byte file.
Files created with `touch` cannot be overwritten and are mostly useful as flags.

For example, `touch` can be used to create a file signifying the completion of analysis on a directory.

```console
$ ./bin/alluxio fs touch /data/yesterday/_DONE_
```

### unmount

The `unmount` command disassociates an Alluxio path with an under storage directory.
Alluxio metadata for the mount point is removed along with any data blocks,
but the under storage system will retain all metadata and data.
See [Unified Namespace]({{ '/en/core-services/Unified-Namespace.html' | relativize_url }}) for more details.

For example, `unmount` can be used to remove an under storage system when the users no longer need
data from that system.

```console
$ ./bin/alluxio fs unmount /s3/data
```

### unpin

The `unpin` command unmarks a file or directory in Alluxio as pinned.
This is a metadata operation and will not evict or delete any data blocks.
Once a file is unpinned, its data blocks can be evicted
from the various Alluxio workers containing the block.

For example, `unpin` can be used when the administrator knows there is a change in the data access pattern.

```console
$ ./bin/alluxio fs unpin /data/yesterday/join-table
```

### unsetTtl

The `unsetTtl` command will remove the TTL attributes of a file or directory in Alluxio.
This is a metadata operation and will not evict or store blocks in Alluxio.
The TTL of a file can later be reset with `setTtl`.

For example, `unsetTtl` can be used if a regularly managed file requires manual management.

```console
$ ./bin/alluxio fs unsetTtl /data/yesterday/data-not-yet-analyzed
```

## Table Operations

```console
$ ./bin/alluxio table
Usage: alluxio table [generic options]
	 [attachdb [-o|--option <key=value>] [--db <alluxio db name>] [--ignore-sync-errors] <udb type> <udb connection uri> <udb db name>]
	 [detachdb <db name>]
	 [ls [<db name> [<table name>]]]
	 [sync <db name>]
	 [transform <db name> <table name>]
	 [transformStatus [<job ID>]]
```

The table subcommand manages the structured data service of Alluxio.

### attachdb

Syntax:
```
attachdb [-o|--option <key=value>] [--db <alluxio db name>] [--ignore-sync-errors] <udb type> <udb connection uri> <udb db name>
```

The `attachdb` command attaches an existing "under database" to the Alluxio catalog. This is
analogous to mounting a under filesystem to the Alluxio filesystem namespace. Once a database is
attached, it will be exposed through the Alluxio catalog.
Here is an example of the usage:

```console
$ ./bin/alluxio table attachdb hive thrift://HOSTNAME:9083 hive_db_name
```

This command will attach the database `hive_db_name` (of type `hive`) from the URI
`thrift://HOSTNAME:9083` to the Alluxio catalog, using the same database name `hive_db_name`.

Here are the attach command options:
  * `--db <alluxio db name>`: specify a different Alluxio database name
  * `--ignore-sync-errors`: ignore sync errors, and keeps the database attached
  * `-o|--option <key=value>`: (multiple) additional properties associated with the attached db and UDB

Here are the additional properties possible for the `-o` options:
  * `udb-<UDB_TYPE>.mount-option.{<UFS_PREFIX>}.<MOUNT_PROPERTY>`: specify a mount option for a
  particular UFS path
    * `<UDB_TYPE>`: the UDB type
    * `<UFS_PREFIX>`: the UFS path prefix that the mount properties are for
    * `<MOUNT_PROPERTY>`: an Alluxio mount property
  * `catalog.db.ignore.udb.tables`: comma-separated list of table names to ignore from the UDB
  * `catalog.db.sync.threads`: number of parallel threads to use to sync with the UDB. If too large,
  the sync may overload the UDB, and if set too low, syncing a database with many tables make take
  a long time. The default is `4`.


### Hive UDB
For the `hive` udb type, during the attach process, the Alluxio catalog will auto-mount all the
table/partition locations in the specified database, to Alluxio. You can supply the mount options
for the possible table locations with the
option `-o udb-hive.mount-option.{scheme/authority}.key=value`.

```console
$ ./bin/alluxio table attachdb hive thrift://HOSTNAME:9083 hive_db_name --db=alluxio_db_name  \
  -o udb-hive.mount-option.{s3a://bucket1}.aws.accessKeyId=abc \
  -o udb-hive.mount-option.{s3a://bucket2}.aws.accessKeyId=123
```

This command will attach the database `hive_db_name` (of type `hive`) from the URI
`thrift://HOSTNAME:9083` to the Alluxio catalog, using the same database name `alluxio_db_name`.
When paths are mounted for `s3a://bucket1`, the mount option `aws.accessKeyId=abc` will be used,
and when paths are mounted for `s3a://bucket2`, the mount option `aws.accessKeyId=123` will be used.

### Glue UDB
For `glue` udb type, there are some additional properties with the `-o` options:
  * `udb-glue.<UDB_PROPERTY>`: specify the UDB options for the Glue UDB. The options
  are as follows:
    * `aws.region`: the glue aws region
    * `aws.catalog.id`: the aws catalog id
    * `aws.accesskey`: the aws access key id
    * `aws.secretkey`: the aws secret key
    * `aws.proxy.protocol`: The protocol(HTTP/HTTPS) to use for connecting to the proxy server
    * `aws.proxy.host`: The proxy host the client will connect through
    * `aws.proxy.port`: The proxy port the client will connect through
    * `aws.proxy.username`: The proxy user name
    * `aws.proxy.password`: The proxy password
    * `table.column.statistics`: Enable table column statistics(true/false)
    * `partition.column.statistics`: Enable partition column statistics(true/false)
    
You can supply the mount options for the `glue` as follows:

```console
$ ./bin/alluxio table attachdb --db alluxio_db_name glue null glue_db_name \
    -o udb-glue.aws.region=<AWS_GLUE_REGION> \
    -o udb-glue.aws.catalog.id=<AWS_CATALOGID> \
    -o udb-glue.aws.accesskey=<AWS_ACCESSKEY_ID> \
    -o udb-glue.aws.secretkey=<AWS_SERCRETKEY_ID>
```

This command will attach the database `glue_db_name` (of type `glue`) to the Alluxio catalog,
using the same database name `alluxio_db_name`. Please notice that `glue` udb does not need the
URI as `hive` udb. When `glue` udb access to AWS glue, the aws region `udb-glue.aws.region`, AWS 
catalog id `udb-glue.aws.catalog.id` and AWS credentials, `udb-glue.aws.accesskey` and 
`udb-glue.aws.secretkey` , need to be provided.

### detachdb

The `detachdb` command is the opposite of the `attachdb` command. Detaching a database will remove
the connection to the under database, and remove it from the Alluxio catalog. Example usage:

```console
$ ./bin/alluxio table detachdb alluxio_db_name
```

This command will detach the database name `alluxio_db_name` from the Alluxio catalog.

### ls

The `ls` command shows information about the Alluxio catalog. Here are some examples:

```console
$ ./bin/alluxio table ls
```

This command without any arguments will show all the databases attached in the system.

```console
$ ./bin/alluxio table ls db_name
```

This command with 1 argument will show all the tables in the `db_name` database.

```console
$ ./bin/alluxio table ls db_name table_name
```

This command with 2 arguments will show the table information of the `table_name` table in
the `db_name` database.

### sync

The `sync` command syncs the metadata of specified database name with the under database.
Here is an example:

```console
$ ./bin/alluxio table sync db_name
```

This will sync the metadata of `db_name` database name with its under database.
The sync will update, add, remove catalog metadata according to the changes found in the underlying
database and tables.
For example, if the under database is `hive`, and the metadata of its tables is updated
in the Hive Metastore (like `MSCK REPAIR` or other commands), then this `sync` command will
update the Alluxio metadata with the updated Hive metadata. 
If an existing Alluxio partition or table is updated and previously had a transformation, then the
transformation is invalidated, and must be re-triggered via the `transform` command.

> If the metadata is NOT updated in the under database, then this sync command will not update
> the Alluxio catalog metadata, even if the data of the table has been updated. For example,
> if files are added to a Hive table but the Hive Metastore is not updated, the sync will not
> detect changes to the metadata.

### transform

The `transform` command will transform a table for improved efficiency when reading the table.
Here is an example usage:

```console
$ ./bin/alluxio table transform db_name table_name [-d <definition>]
```

This command will invoke a transformation on the table. The transformation is performed
asynchronously, and will coalesce to a fewer number of files, and convert into the parquet file
format.

> In 2.1.0, the supported file formats which can be transformed are: parquet and csv
> file formats. The resulting transformations are in the parquet file format. Additional formats
> for input and output will be implemented in future versions.
> For the coalesce feature, by default it will coalesce into a maximum of 100 files, 
> with each file no smaller than 2GB.
 
The definition format takes a form of configuration separated by semicolon and specifies the details of the output
format. Available configurations are:

```console
file.count.max=<num> (maximum number of files in transformed output)
file.size.min=<num> (minimum file size in bytes of the output)
```

### transformStatus

The `transformStatus` command will display information about a table transformation.
Here is an example usage:

```console
$ ./bin/alluxio table transformStatus transform_id
```

This command will display status details on the transformation identified by `transform_id`.
