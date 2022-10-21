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

### format

The `format` command formats the Alluxio master and all its workers.

If `-s` specified, only format if under storage is local and does not already exist

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio,
including cached data and any metadata information.
Data in under storage will not be changed.

> Warning: `format` is required when you run Alluxio for the first time.
`format` should only be called while the cluster is not running.

```console
$ ./bin/alluxio format
$ ./bin/alluxio format -s
```

### formatJournal

The `formatJournal` command formats the Alluxio master journal on this host.

The Alluxio master stores various forms of metadata, including:
- file system operations
- where files are located on workers
- journal transactions
- under storage file metadata

All this information is deleted if `formatJournal` is run.,

> Warning: `formatJournal` should only be called while the cluster is not running.


```console
$ ./bin/alluxio formatJournal
```

### formatMasters

The `formatMasters` command formats the Alluxio masters.

This command defers to [formatJournal](#formatjournal),
but if the UFS is an embedded journal it will format all master nodes listed in the 'conf/masters' file
instead of just this host.

The Alluxio master stores various forms of metadata, including:
- file system operations
- where files are located on workers
- journal transactions
- under storage file metadata

All this information is deleted if `formatMasters` is run.,

> Warning: `formatMasters` should only be called while the cluster is not running.


```console
$ ./bin/alluxio formatMasters
```

### formatWorker

The `formatWorker` command formats the Alluxio worker on this host.

An Alluxio worker caches files and objects.

`formatWorker` deletes all the cached data stored in this worker node.
Data in under storage will not be changed.

> Warning: `formatWorker` should only be called while the cluster is not running.

```console
$ ./bin/alluxio formatWorker
```

### bootstrapConf

The `bootstrapConf` command generates the bootstrap configuration file
`${ALLUXIO_HOME}/conf/alluxio-site.properties` with `alluxio.master.hostname`
set to the passed in value if the configuration file does not exist.

<!-- Generated configuration file is empty except for "alluxio.master.hostname" -->

```console
$ ./bin/alluxio bootstrapConf <ALLUXIO_MASTER_HOSTNAME>
```

> Note: This command does not require the Alluxio cluster to be running.

### fs

See [File System Operations](#file-system-operations).

### fsadmin

The `fsadmin` command is meant for administrators of the Alluxio cluster.
It provides added tools for diagnostics and troubleshooting.
For more information see the [Admin CLI main page]({{ '/en/operation/Admin-CLI.html' | relativize_url }}).

> Note: This command requires the Alluxio cluster to be running.

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

> Note: This command does not require the Alluxio cluster to be running.

### job

The `job` command is a tool for interacting with the job service.

The usage is `job [generic options]`
where `[generic options]` can be one of the following values:
* `leader`: Prints the hostname of the job master service leader.
* `ls`: Prints the IDs of the most recent jobs, running and finished, in the history up to the capacity set in `alluxio.job.master.job.capacity`.
* `stat [-v] <id>`:Displays the status info for the specific job. Use -v flag to display the status of every task.
* `cancel <id>`: Cancels the job with the corresponding id asynchronously.

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

> Note: This command requires the Alluxio cluster to be running.

### logLevel

The `logLevel` command returns the current value of or updates the log level of a particular class
on specific instances. Users are able to change Alluxio server-side log levels at runtime.

The command follows the format `alluxio logLevel --logName=NAME [--target=<master|workers|job_master|job_workers|host:webPort[:role]>] [--level=LEVEL]`,
where:
* `--logName <arg>` indicates the logger's class (e.g. `alluxio.master.file.DefaultFileSystemMaster`)
* `--target <arg>` lists the Alluxio master or workers to set.
The target could be of the form `<master|workers|job_master|job_workers|host:webPort[:role]>` and multiple targets can be listed as comma-separated entries.
`role` can be one of `master|worker|job_master|job_worker`. Using the `role` option is useful when an Alluxio process
is configured to use a non-standard web port (e.g. if an Alluxio master does not use 19999 as its web port).
The default target value is the primary master, primary job master, all workers and job workers.
* `--level <arg>` If provided, the command changes to the given logger level,
otherwise it returns the current logger level.

See [here]({{ '/en/operation/Basic-Logging.html#modifying-server-logging-at-runtime' | relativize_url }})
for more examples.

> Note: This command requires the Alluxio cluster to be running.
> You are not able to set the logger level on the standby masters.
> The standby masters/job masters do not have a running web server.
> So they are not accepting the requests from this command.
> If you want to modify the logger level for standby masters,
> update the `log4j.properties` and restart the process.

### runClass

The `runClass` command runs the main method of an Alluxio class.

For example, to run the multi-mount demo:
```console
$ ./bin/alluxio runClass alluxio.examples.MultiMount <HDFS_URL>
```

### runTest

The `runTest` command runs end-to-end tests on an Alluxio cluster.

The usage is `runTest [--directory <path>] [--operation <operation type>] [--readType <read type>] [--writeType <write type>]`.
  * `--directory`
    Alluxio path for the tests working directory.
    Default: `${ALLUXIO_HOME}`
  * `--operation`
    The operation to test, one of BASIC or BASIC_NON_BYTE_BUFFER. By default
    both operations are tested.
  * `--readType`
    The read type to use, one of NO_CACHE, CACHE, CACHE_PROMOTE. By default all readTypes are tested.
  * `--writeType`
    The write type to use, one of MUST_CACHE, CACHE_THROUGH, THROUGH, ASYNC_THROUGH. By default all writeTypes are tested.

```console
$ ./bin/alluxio runTest

$ ./bin/alluxio runTest --operation BASIC --readType CACHE --writeType MUST_CACHE
```

> Note: This command requires the Alluxio cluster to be running.

### runTests

The `runTests` command runs all the end-to-end tests on an Alluxio cluster to provide a comprehensive sanity check.

This command is equivalent to running [runTest](#runtest) with all the default flag values.

```console
$ ./bin/alluxio runTests
```

> Note: This command requires the Alluxio cluster to be running.

### runJournalCrashTest

The `runJournalCrashTest` simulates a failover to test recovery from the journal.

> Note: This command will stop any Alluxio services running on the machine.

### runHmsTests

The `runHmsTests` aims to validate the configuration, connectivity, and permissions of an existing hive metastore
which is an important component in compute workflows with Alluxio.

* `-h` provides detailed guidance.
* `-m <hive_metastore_uris>` (required) the full hive metastore uris to connect to an existing hive metastore.
* `-d <database_name>` the database to run tests against. Use `default` database if not provided.
* `-t [table_name_1,table_name_2,...]` tables to run tests against. Run tests against five out of all tables in the given database if not provided.
* `-st <timeout>` socket timeout of hive metastore client in minutes.

```console
$ ./bin/alluxio runHmsTests -m thrift://<hms_host>:<hms_port> -d tpcds -t store_sales,web_sales
```

This tool is suggested to run from compute application environments and checks
* if the given hive metastore uris are valid
* if the hive metastore client connection can be established with the target server
* if hive metastore client operations can be run against the given database and tables

> Note: This command does not require the Alluxio cluster to be running.

### runHdfsMountTests

The `runHdfsMountTests` command aims to validate the configuration, connectivity and permissions of an HDFS path.
It validates various aspects for connecting to HDFS with the given Alluxio configurations and identifies issues
before the path is mounted to Alluxio.
This tool will validate a few criteria and return the feedback.
If a test failed, advice will be given correspondingly on how the user can rectify the setup.

Usage: `runHdfsMountTests [--readonly] [--shared] [--option <key=val>] <hdfsURI>`
* `--help` provides detailed guidance.
* `--readonly` specifies the mount point should be readonly in Alluxio.
* `--shared` specifies the mount point should be accessible for all Alluxio users.
* `--option <key>=<val>` passes an property to this mount point.
* `<hdfs-path>` (required) specifies the HDFS path you want to validate (then mount to Alluxio)

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
> This command does not require the Alluxio cluster to be running.

### runUfsIOTest

The `runUfsIOTest` command measures the read/write IO throughput from Alluxio cluster to the target HDFS.

Usage: `runUfsIOTest --path <hdfs-path> [--io-size <io-size>] [--threads <thread-num>] [--cluster] [--cluster-limit <worker-num>] --java-opt <java-opt>`
* `-h, --help` provides detailed guidance.
* `--path <hdfs-path>` (required) specifies the path to write/read temporary data in.
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

> Note: This command requires the Alluxio cluster to be running.

### runUfsTests

The `runUfsTests` aims to test the integration between Alluxio and the given UFS. UFS tests
validate the semantics Alluxio expects of the UFS.

Usage: `runUfsTests --path <ufs_path>`
* `--help` provides detailed guidance.
* `--path <ufs_path>` (required) the full UFS path to run tests against.

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
  -Ds3a.accessKeyId=<access_key> -Ds3a.secretKey=<secret_key> \
  -Dalluxio.underfs.s3.endpoint=<endpoint_url> -Dalluxio.underfs.s3.disable.dns.buckets=true
```

> Note: This command does not require the Alluxio cluster to be running.

### readJournal

The `readJournal` command parses the current journal and outputs a human readable version to the local folder.
Note this command may take a while depending on the size of the journal.
Note that Alluxio master is required to stop before reading the local embedded journal.

* `-help` provides detailed guidance.
* `-start <arg>` the start log sequence number (exclusive). (Default: `0`)
* `-end <arg>` the end log sequence number (exclusive). (Default: `+inf`)
* `-inputDir <arg>` the input directory on-disk to read journal content from. (Default: Read from system configuration)
* `-outputDir <arg>` the output directory to write journal content to. (Default: journal_dump-${timestamp})
* `-master <arg>` (advanced) the name of the master (e.g. FileSystemMaster, BlockMaster). (Default: "FileSystemMaster")

```console
$ ./bin/alluxio readJournal
Dumping journal of type EMBEDDED to /Users/alluxio/journal_dump-1602698211916
2020-10-14 10:56:51,960 INFO  RaftStorageDirectory - Lock on /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/in_use.lock acquired by nodename 78602@alluxio-user
2020-10-14 10:56:52,254 INFO  RaftJournalDumper - Read 223 entries from log /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/current/log_0-222.
```

> Note: This command requires that the Alluxio cluster is **NOT** running.

### upgradeJournal

The `upgradeJournal` command upgrades an Alluxio journal version 0 (Alluxio version < 1.5.0)
to an Alluxio journal version 1 (Alluxio version >= 1.5.0).

`-journalDirectoryV0 <arg>` will provide the v0 journal persisted location.\
It is assumed to be the same as the v1 journal directory if not set.

```console
$ ./bin/alluxio upgradeJournal
```

> Note: This command does not require the Alluxio cluster to be running.

### killAll

The `killAll` command kills all processes containing the specified word.
> Note: This kills non-Alluxio processes as well.

### copyDir

The `copyDir` command copies the directory at `PATH` to all master nodes listed in `conf/masters`
and all worker nodes listed in `conf/workers`.

```console
$ ./bin/alluxio copyDir conf/alluxio-site.properties
```

> Note: This command does not require the Alluxio cluster to be running.

### clearCache

The `clearCache` command drops the OS buffer cache.

> Note: This command does not require the Alluxio cluster to be running.

### docGen

The `docGen` command autogenerates documentation based on the current source code.

Usage: `docGen [--metric] [--conf]`
* `--metric` flag indicates to generate Metric docs
* `--conf` flag indicates to generate Configuration docs

Supplying neither flag will default to generating both docs.

> Note: This command does not require the Alluxio cluster to be running.

### table

See [Table Operations](#table-operations).

### version

The `version` command prints Alluxio version.

Usage: `version --revision [revision_length]`
* `-r,--revision [revision_length]` Prints the git revision along with the Alluxio version. Optionally specify the revision length.

```console
$ ./bin/alluxio version
```

> Note: This command does not require the Alluxio cluster to be running.

### validateConf

The `validateConf` command validates the local Alluxio configuration files, checking for common misconfigurations.

```console
$ ./bin/alluxio validateConf
```

> Note: This command does not require the Alluxio cluster to be running.

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

> Note: This command does not require the Alluxio cluster to be running.

### collectInfo

The `collectInfo` command collects information to troubleshoot an Alluxio cluster.
For more information see the [collectInfo command page]({{ '/en/operation/Troubleshooting.html#alluxio-collectinfo-command' | relativize_url }}).

> Note: This command does not require the Alluxio cluster to be running.
> But if the cluster is not running, this command will fail to gather some information from it.

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

> Note: This command requires the Alluxio cluster to be running.

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

If the `-t <thread count>` option is specified, the provided number of threads will be used when
repairing consistency. Defaults to the number of CPU cores available,
* This option has no effect if `-r` is not specified

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
If a directory is specified, the directory and all its contents will be copied recursively
(parallel at file level up to the number of available threads).

Usage: `copyFromLocal [--thread <num>] [--buffersize <bytes>] <src> <remoteDst>`
* `--thread <num>` (optional) Number of threads used to copy files in parallel, default value is CPU cores * 2
* `--buffersize <bytes>` (optional) Read buffer size in bytes, default is 8MB when copying from local and 64MB when copying to local
* `<src>` file or directory path on the local filesystem
* `<remoteDst>` file or directory path on the Alluxio filesystem

For example, `copyFromLocal` can be used as a quick way to inject data into the system for processing:

```console
$ ./bin/alluxio fs copyFromLocal /local/data /input
```

### copyToLocal

The `copyToLocal` command copies a file in Alluxio to the local file system.
If a directory is specified, the directory and all its contents will be copied recursively.

Usage: `copyToLocal [--buffersize <bytes>] <src> <localDst>`
* `--buffersize <bytes>` (optional) file transfer buffer size in bytes
* `<src>` file or directory path on the Alluxio filesystem
* `<localDst>` file or directory path on the local filesystem

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

Usage: `count [-h] <dir>`
* `-h` (optional) print sizes in human readable format (e.g. 1KB 234MB 2GB)
* `<dir>` file or directory path in the Alluxio filesystem

```console
$ ./bin/alluxio fs count -h /LICENSE
File Count               Folder Count             Folder Size
1                        0                        26.41KB
```

`count` is best utilized when the user has some predefined naming conventions for their files.
For example, if data files are stored by their date, `count` can be used to determine the number of
data files and their total size for any date, month, or year.

### cp

The `cp` command copies a file or directory in the Alluxio file system
or between the local file system and Alluxio file system.

Scheme `file://` indicates the local file system
whereas scheme `alluxio://` or no scheme indicates the Alluxio file system.

If the `-R` option is used and the source designates a directory,
`cp` copies the entire subtree at source to the destination.

Usage: `cp [--thread <num>] [--buffersize <bytes>] [--preserve] <src> <dst>`
* `--thread <num>` (optional) Number of threads used to copy files in parallel, default value is CPU cores * 2
* `--buffersize <bytes>` (optional) Read buffer size in bytes, default is 8MB when copying from local and 64MB when copying to local
* `--preserve` (optional) Preserve file permission attributes when copying files. All ownership, permissions and ACLs will be preserved.
* `<src>` source file or directory path
* `<dst>` destination file or directory path

For example, `cp` can be used to copy files between under storage systems.

```console
$ ./bin/alluxio fs cp /hdfs/file1 /s3/
```

### distributedCp

The `distributedCp` command copies a file or directory in the Alluxio file system distributed across workers
using the job service. By default, the command runs synchronously and the user will get a `JOB_CONTROL_ID` after the command successfully submits the job to be executed.
The command will wait until the job is complete, at which point the user will see the list of files copied and statistics on which files completed or failed.
The command can also run in async mode with the `--async` flag. Similar to before, the user will get a `JOB_CONTROL_ID` after the command successfully submits the job.
The difference is that the command will not wait for the job to finish. 
Users can use the [`getCmdStatus`](#getCmdStatus) command with the `JOB_CONTROL_ID` as an argument to check detailed status information about the job.

If the source designates a directory, `distributedCp` copies the entire subtree at source to the destination.

Options:
* `--active-jobs`: Limits how many jobs can be submitted to the Alluxio job service at the same time.
Later jobs must wait until some earlier jobs to finish. The default value is `3000`.
A lower value means slower execution but also being nicer to the other users of the job service.
* `--overwrite`: Whether to overwrite the destination. Default is true.
* `--batch-size`: Specifies how many files to be batched into one request. The default value is `20`. Notice that if some task failed in the batched job, the whole batched job would fail with some completed tasks and some failed tasks.
* `--async`: Specifies whether to wait for command execution to finish. If not explicitly shown then default to run synchronously.
```console
$ ./bin/alluxio fs distributedCp --active-jobs 2000 /data/1023 /data/1024
Sample Output:
Please wait for command submission to finish..
Submitted successfully, jobControlId = JOB_CONTROL_ID_1
Waiting for the command to finish ...
Get command status information below:
Successfully copied path /data/1023/$FILE_PATH_1
Successfully copied path /data/1023/$FILE_PATH_2
Successfully copied path /data/1023/$FILE_PATH_3
Total completed file count is 3, failed file count is 0
Finished running the command, jobControlId = JOB_CONTROL_ID_1
```

```console
# Turn on async submission mode. Run this command to get JOB_CONTROL_ID, then use getCmdStatus to check command detailed status.
$ ./bin/alluxio fs distributedCp /data/1023 /data/1025 --async
Sample Output:
Entering async submission mode.
Please wait for command submission to finish..
Submitted migrate job successfully, jobControlId = JOB_CONTROL_ID_2
```

### distributedLoad

The `distributedLoad` command loads a file or directory from the under storage system into Alluxio storage distributed
across workers using the job service. The job is a no-op if the file is already loaded into Alluxio.
By default, the command runs synchronously and the user will get a `JOB_CONTROL_ID` after the command successfully submits the job to be executed.
The command will wait until the job is complete, at which point the user will see the list of files loaded and statistics on which files completed or failed.
The command can also run in async mode with the `--async` flag. Similar to before, the user will get a `JOB_CONTROL_ID` after the command successfully submits the job.
The difference is that the command will not wait for the job to finish.
Users can use the [`getCmdStatus`](#getCmdStatus) command with the `JOB_CONTROL_ID` as an argument to check detailed status information about the job.

If `distributedLoad` is run on a directory, files in the directory will be recursively loaded and each file will be loaded
on a random worker.

Options:

* `--replication`: Specifies how many workers to load each file into. The default value is `1`.
* `--active-jobs`: Limits how many jobs can be submitted to the Alluxio job service at the same time.
Later jobs must wait until some earlier jobs to finish. The default value is `3000`.
A lower value means slower execution but also being nicer to the other users of the job service.
* `--batch-size`: Specifies how many files to be batched into one request. The default value is `20`. Notice that if some task failed in the batched job, the whole batched job would fail with some completed tasks and some failed tasks.
* `--host-file <host-file>`: Specifies a file contains worker hosts to load target data, each line has a worker host.
* `--hosts`: Specifies a list of worker hosts separated by comma to load target data.
* `--excluded-host-file <host-file>`: Specifies a file contains worker hosts which shouldn't load target data, each line has a worker host.
* `--excluded-hosts`: Specifies a list of worker hosts separated by comma which shouldn't load target data.
* `--locality-file <locality-file>`: Specifies a file contains worker locality to load target data, each line has a locality.
* `--locality`: Specifies a list of worker locality separated by comma to load target data.
* `--excluded-locality-file <locality-file>`: Specifies a file contains worker locality which shouldn't load target data, each line has a worker locality.
* `--excluded-locality`: Specifies a list of worker locality separated by comma which shouldn't load target data.
* `--index`: Specifies a file that lists all files to be loaded
* `--passive-cache`: Specifies using direct cache request or passive cache with read(old implementation)
* `--async`: Specifies whether to wait for command execution to finish. If not explicitly shown then default to run synchronously.

```console
$ ./bin/alluxio fs distributedLoad --replication 2 --active-jobs 2000 /data/today
Sample Output:
Please wait for command submission to finish..
Submitted successfully, jobControlId = JOB_CONTROL_ID_3
Waiting for the command to finish ...
Get command status information below:
Successfully loaded path /data/today/$FILE_PATH_1
Successfully loaded path /data/today/$FILE_PATH_2
Successfully loaded path /data/today/$FILE_PATH_3
Total completed file count is 3, failed file count is 0
Finished running the command, jobControlId = JOB_CONTROL_ID_3
```

```console
# Turn on async submission mode. Run this command to get JOB_CONTROL_ID, then use getCmdStatus to check command detailed status.
$ ./bin/alluxio fs distributedLoad /data/today --async
Sample Output:
Entering async submission mode.
Please wait for command submission to finish..
Submitted distLoad job successfully, jobControlId = JOB_CONTROL_ID_4
```

Or you can include some workers or exclude some workers by using options `--host-file <host-file>`, `--hosts`, `--excluded-host-file <host-file>`,
`--excluded-hosts`, `--locality-file <locality-file>`, `--locality`, `--excluded-host-file <host-file>` and `--excluded-locality`.

Note: Do not use `--host-file <host-file>`, `--hosts`, `--locality-file <locality-file>`, `--locality` with
`--excluded-host-file <host-file>`, `--excluded-hosts`, `--excluded-host-file <host-file>`, `--excluded-locality` together.

```console
# Only include host1 and host2
$ ./bin/alluxio fs distributedLoad /data/today --hosts host1,host2
# Only include the workset from host file /tmp/hostfile
$ ./bin/alluxio fs distributedLoad /data/today --host-file /tmp/hostfile
# Include all workers except host1 and host2 
$ ./bin/alluxio fs distributedLoad /data/today --excluded-hosts host1,host2
# Include all workers except the workerset in the excluded host file /tmp/hostfile-exclude
$ ./bin/alluxio fs distributedLoad /data/today --excluded-file /tmp/hostfile-exclude
# Include workers which's locality identify belong to ROCK1 or ROCK2
$ ./bin/alluxio fs distributedLoad /data/today --locality ROCK1,ROCK2
# Include workers which's locality identify belong to the localities in the locality file
$ ./bin/alluxio fs distributedLoad /data/today --locality-file /tmp/localityfile
# Include all workers except which's locality belong to ROCK1 or ROCK2 
$ ./bin/alluxio fs distributedLoad /data/today --excluded-locality ROCK1,ROCK2
# Include all workers except which's locality belong to the localities in the excluded locality file
$ ./bin/alluxio fs distributedLoad /data/today --excluded-locality-file /tmp/localityfile-exclude

# Conflict cases
# The `--hosts` and `--locality` are `OR` relationship, so host2,host3 and workers in ROCK2,ROCKS3 will be included.
$ ./bin/alluxio fs distributedLoad /data/today --locality ROCK2,ROCK3 --hosts host2,host3
# The `--excluded-hosts` and `--excluded-locality` are `OR` relationship, so host2,host3 and workers in ROCK2,ROCKS3 will be excluded.
$ ./bin/alluxio fs distributedLoad /data/today --excluded-hosts host2,host3 --excluded-locality ROCK2,ROCK3
```

See examples for [Tiered Locality Example]({{ '/en/operation/Tiered-Locality.html' | relativize_url }}#Example)

### distributedMv

The `distributedMv` command moves a file or directory in the Alluxio file system distributed across workers
using the job service.

If the source designates a directory, `distributedMv` moves the entire subtree at source to the destination.

```console
$ ./bin/alluxio fs distributedMv /data/1023 /data/1024
```

### du

The `du` command outputs the total size and amount stored in Alluxio of files and folders.
If a directory is specified, it will display the sizes of all files in this directory.

Usage: `du [-s] [-h] [--memory] [-g] <dir>`
* `-s` (optional) display the aggregate summary of file lengths being displayed
* `-h` (optional) print sizes in human readable format (e.g. 1KB 234MB 2GB)
* `-m,--memory` (optional) display the in memory size and in memory percentage
* `-g` (optional) display information for In-Alluxio data size under the path, grouped by worker
* `<dir>` file or directory path in the Alluxio filesystem

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

Usage: `free [-f]`
* `-f` force to free files even pinned

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

### getCmdStatus

The `getCmdStatus` command returns the detailed distributed command status based on a given JOB_CONTROL_ID.
The detailed status includes:
1. Successfully loaded or copied file paths.
2. Statistics on the number of successful and failed file paths.
3. Failed file paths, logged in a separate csv file.

For example, `getCmdStatus` can be used to check what files are loaded in a distributed command, and how many succeeded or failed.

```console
$ ./bin/alluxio job getCmdStatus $JOB_CONTROL_ID
Sample Output:
Get command status information below:
Successfully loaded path $FILE_PATH_1
Successfully loaded path $FILE_PATH_2
Total completed file count is 2, failed file count is 0
```

### getfacl

The `getfacl` command returns the ACL entries for a specified file or directory.

For example, `getfacl` can be used to verify that an ACL is changed successfully after a call to `setfacl`.

```console
$ ./bin/alluxio fs getfacl /testdir/testfile
```

### getSyncPathList

The `getSyncPathList` command gets all the paths that are under active syncing right now.

```console
$ ./bin/alluxio fs getSyncPathList
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
`-f` is equivalent to `-Dalluxio.user.file.metadata.sync.interval=0`.
* `-h` option displays file sizes in human-readable formats.
* `-p` option lists all pinned files.
* `-R` option also recursively lists child directories, displaying the entire subtree starting from the input path.
* `--sort` sorts the result by the given option. Possible values are size, creationTime, inMemoryPercentage, lastModificationTime, lastAccessTime and path.
* `-r` reverses the sorting order.
* `--timestamp` display the timestamp of the given option. Possible values are creationTime, lastModificationTime, and lastAccessTime.
The default option is lastModificationTime.

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

Metadata sync is an expensive operation. A rough estimation is metadata sync
on 1 million files will consume 2GB heap until the sync operation is complete.
Therefore, we recommend not using forced sync to avoid accidental repeated sync operations.
It is recommended to always specify a non-zero sync interval for metadata sync, so
even if the sync is repeatedly triggered, the paths that have just been sync-ed can be identified and skipped. 
```console
# Should be avoided
$ ./bin/alluxio fs ls -f -R /s3/data

# Recommended. This will not sync files repeatedly in 1 minute.
$ ./bin/alluxio fs ls -Dalluxio.user.file.metadata.sync.interval=1min -R /s3/data
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
  --option s3a.accessKeyId=<accessKeyId> \
  --option s3a.secretKey=<secretKey> \
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

Usage: `persist [-p <parallelism>] [-t <timeout>] [-w <wait time>] <dir>`
* `-p,--parallelism <parallelism>]` (optional) Number of concurrent persist operations. (Default: 4)
* `-t,--timeout <timeout>` (optional) Time in milliseconds for a single file persist to time out. (Default: 20 minutes)
* `-w,--wait <wait time>` (optional) The time to wait in milliseconds before persisting. (Default: 0)
* `<dir>` file or directory path in the Alluxio filesystem

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
before attempting to delete persisted directories. We recommend always using the `-U` option for the best performance and resource efficiency.
* Adding `--alluxioOnly` option removes data and metadata from Alluxio space only.
The under storage system will not be affected.

```console
# Remove a file from Alluxio space and the under storage system
$ ./bin/alluxio fs rm /tmp/unused-file
# Remove a file from Alluxio space only
$ ./bin/alluxio fs rm --alluxioOnly /tmp/unused-file2
```

When deleting only from Alluxio but leaving the files in UFS, we recommend using `-U` and `-Dalluxio.user.file.metadata.sync.interval=-1`
to skip the metadata sync and the UFS check. This will save time and memory consumption on the Alluxio master.
```console
$ bin/alluxio fs rm -R -U --alluxioOnly -Dalluxio.user.file.metadata.sync.interval=-1 /dir
```

When deleting a large directory (with millions of files) recursively both from Alluxio and UFS,
the operation is expensive. 

We recommend doing the deletion in the following way:
1. Perform a direct sanity check against the UFS path with the corresponding file system API
or CLI to make sure everything can be deleted safely. 
For example if the UFS is HDFS, use `hdfs dfs -ls -R /dir` to list the UFS files and check.
We do not recommend doing this sanity check from Alluxio using a command like `alluxio fs ls -R -f /dir`,
because the loaded file metadata will be deleted anyway, and the expensive metadata sync operation
will essentially be wasted.

2. Issue the deletion from Alluxio to delete files from both Alluxio and the UFS:
```console
# Disable the sync and skip the UFS check, to reduce memory consumption on the master side
$ bin/alluxio fs rm -R -U -Dalluxio.user.file.metadata.sync.interval=-1 /dir
```

Per 1 million files deleted, the memory overhead can be estimated as follows:
* If both metadata sync and UFS check are disabled, recursively deleting from Alluxio only will hold 2GB JVM heap memory until the deletion completes.
* If files are also deleted from UFS, there will not be extra heap consumption but the operation will take longer to complete.
* If metadata sync is enabled, there will be another around 2GB overhead on the JVM heap until the operation completes.
* If UFS check is enabled, there will another around 2GB overhead on the JVM heap until the operation completes.

Using this example as a guideline, estimate the total additional memory overhead as a proportion to the number of files to be deleted. 
Ensure that the leading master has sufficient available heap memory to perform the operation before issuing a large recursive delete command.
A general good practice is to break deleting a large directory into deleting each individual children directories.

### setfacl

The `setfacl` command modifies the access control list associated with a specified file or directory.

* The`-R` option applies operations to all files and directories recursively.
* The `set` option fully replaces the ACL while discarding existing entries.
New ACL must be a comma separated list of entries, and must include user, group,
and other for compatibility with permission bits.
* The `-m` option modifies the ACL by adding/overwriting new entries.
* The `-x` option removes specific ACL entries.
* The `-b` option removes all ACL entries, except for the base entries.
* The `-d` option indicates that operations apply to the default ACL
* The `-k` option removes all the default ACL entries.

For example, `setfacl` can be used to give read and execute permissions to a user named `testuser`.

```console
$ ./bin/alluxio fs setfacl -m "user:testuser:r-x" /testdir/testfile
```

### setReplication

The `setReplication` command sets the max and/or min replication level of a file or all files under
a directory recursively. This is a metadata operation and will not cause any replication to be
created or removed immediately. The replication level of the target file or directory will be
changed automatically in background.

* The `--min` option specifies the minimal replication level
* The `--max` optional specifies the maximal replication level. Specify -1 as the argument of
`--max` option to indicate no limit of the maximum number of replicas.
* If the specified path is a directory and `-R` is specified, it will recursively set all files in this directory.

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
**The default action, delete, deletes the file or directory from both Alluxio and the under storage system**,
whereas the action `free` frees the file from Alluxio even if pinned.

For example, `setTtl` with action `delete` cleans up files the administrator knows are unnecessary after a period of time,
or with action `free` just remove the contents from Alluxio to make room for more space in Alluxio.

```console
# After 1 day, delete the file in Alluxio and UFS
$ ./bin/alluxio fs setTtl /data/good-for-one-day 86400000
# After 1 day, free the file from Alluxio
$ ./bin/alluxio fs setTtl --action free /data/good-for-one-day 86400000
```

### startSync

The `startSync` command starts the automatic syncing process of the specified path.

```console
$ ./bin/alluxio fs startSync /data/2014
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

### stopSync

The `stopSync` command stops the automatic syncing process of the specified path.

```console
$ ./bin/alluxio fs stopSync /data/2014
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

If there are files under the mount point, the `unmount` operation will implicitly delete those files from Alluxio.
See the [rm command]({{ '/en/operation/User-CLI.html#rm' | relativize_url }}) for how to estimate the memory consumption.
It is recommended to remove those files in Alluxio first, before the `unmount`.

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

### updateMount

The `updateMount` command updates options for a mount point while keeping the Alluxio metadata under the path.

Usage: `updateMount [--readonly] [--shared] [--option <key=val>] <alluxioPath>`
* `--readonly` (optional) mount point is readonly in Alluxio
* `--shared` (optional) mount point is shared
* `--option <key>=<val>` (optional) options for this mount point.
For security reasons, no options from existing mount point will be inherited.
* `<alluxioPath>` Directory path in the Alluxio filesystem

