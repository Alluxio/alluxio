---
layout: global
title: User Command Line Interface
---

Alluxio's command line interface provides users with basic file system operations. You can invoke
the following command line utility to get all the subcommands:

```shell
$ ./bin/alluxio
Usage: alluxio [COMMAND]
       [format [-s]]
       [getConf [key]]
       [logLevel]
       [runTests]
       ...
```

## General operations

This section lists usages and examples of general Alluxio operations

### format

The `format` command formats the Alluxio master and all its workers.

If `-s` specified, only format if under storage is local and does not already exist

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio,
including cached data and any metadata information.
Data in under storage will not be changed.

> Warning: `format` is required when you run Alluxio for the first time.
`format` should only be called while the cluster is not running.

```shell
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


```shell
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


```shell
$ ./bin/alluxio formatMasters
```

### formatWorker

The `formatWorker` command formats the Alluxio worker on this host.

An Alluxio worker caches files and objects.

`formatWorker` deletes all the cached data stored in this worker node.
Data in under storage will not be changed.

> Warning: `formatWorker` should only be called while the cluster is not running.

```shell
$ ./bin/alluxio formatWorker
```

### fs

See [File System Operations](#file-system-operations).

### getConf

The `getConf` command prints the configured value for the given key.
If the key is invalid, it returns a nonzero exit code.
If the key is valid but isn't set,  an empty string is printed.
If no key is specified, the full configuration is printed.

**Options:**

* `--master` option prints any configuration properties used by the master.
* `--source` option prints the source of the configuration properties.
* `--unit <arg>` option displays the configuration value in the given unit.
For example, with `--unit KB`, a configuration value of `4096B` returns as `4`,
and with `--unit S`, a configuration value of `5000ms` returns as `5`.
Possible unit options include B, KB, MB, GB, TP, PB as units of byte size and
MS, S, M, H, D as units of time.

```shell
# Display all the current node configuration
$ ./bin/alluxio getConf
```

```shell
# Display the value of a property key
$ ./bin/alluxio getConf alluxio.master.hostname
```

```shell
# Display the configuration of the current running Alluxio leading master
$ ./bin/alluxio getConf --master
```

```shell
# Display the source of the configuration
$ ./bin/alluxio getConf --source
```

```shell
# Display the values in a given unit
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

Print the hostname of the job master service leader:
```shell
$ ./bin/alluxio job leader
```

Print the IDs, job names, and completion status of the most recently created jobs:
```shell
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
```

Display the status info for the specific job:
```shell
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
```

Cancel the job asynchronously based on a specific job:
```shell
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

See [here]({{ '/en/administration/Basic-Logging.html#modifying-server-logging-at-runtime' | relativize_url }})
for more examples.

> Note: This command requires the Alluxio cluster to be running.
> You are not able to set the logger level on the standby masters.
> The standby masters/job masters do not have a running web server.
> So they are not accepting the requests from this command.
> If you want to modify the logger level for standby masters,
> update the `log4j.properties` and restart the process.

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

```shell
$ ./bin/alluxio readJournal

Dumping journal of type EMBEDDED to /Users/alluxio/journal_dump-1602698211916
2020-10-14 10:56:51,960 INFO  RaftStorageDirectory - Lock on /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/in_use.lock acquired by nodename 78602@alluxio-user
2020-10-14 10:56:52,254 INFO  RaftJournalDumper - Read 223 entries from log /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/current/log_0-222.
```

> Note: This command requires that the Alluxio cluster is **NOT** running.

### killAll

The `killAll` command kills all processes containing the specified word.
> Note: This kills non-Alluxio processes as well.

### copyDir

The `copyDir` command copies the directory at `PATH` to all master nodes listed in `conf/masters`
and all worker nodes listed in `conf/workers`.

```shell
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

### version

The `version` command prints Alluxio version.

Usage: `version --revision [revision_length]`
* `-r,--revision [revision_length]` Prints the git revision along with the Alluxio version. Optionally specify the revision length.

```shell
$ ./bin/alluxio version
```

> Note: This command does not require the Alluxio cluster to be running.

### validateConf

The `validateConf` command validates the local Alluxio configuration files, checking for common misconfigurations.

```shell
$ ./bin/alluxio validateConf
```

> Note: This command does not require the Alluxio cluster to be running.

### collectInfo

The `collectInfo` command collects information to troubleshoot an Alluxio cluster.
For more information see the [collectInfo command page]({{ '/en/administration/Troubleshooting.html#alluxio-collectinfo-command' | relativize_url }}).

> Note: This command does not require the Alluxio cluster to be running.
> But if the cluster is not running, this command will fail to gather some information from it.

## File System Operations

```shell
$ ./bin/alluxio fs

Usage: alluxio fs [generic options]
       [cat <path>]
       [checkConsistency [-r] <Alluxio path>]
       ...
```

For `fs` subcommands that take Alluxio URIs as argument (e.g. `ls`, `mkdir`), the argument should
be either a complete Alluxio URI, such as `alluxio://<master-hostname>:<master-port>/<path>`,
or a path without its header, such as `/<path>`, to use the default hostname and port set in the
`conf/alluxio-site.properties`.

> Note: This command requires the Alluxio cluster to be running.

>**Wildcard Input:**
>
>Most of the commands which require path components allow wildcard arguments for ease of use. For
>example:
>
>```shell
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
>```shell
>$ ./bin/alluxio fs cat /\\*
>```
>
>Note the double escape; this is because the shell script will eventually call a java program
>which should have the final escaped parameters (`cat /\\*`).

### cat

The `cat` command prints the contents of a file in Alluxio to the shell.
If you wish to copy the file to your local file system, `copyToLocal` should be used.

For example, when testing a new computation job, `cat` can be used as a quick way to check the output:

```shell
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

```shell
# List each inconsistent file or directory
$ ./bin/alluxio fs checkConsistency /
```

```shell
# Repair the inconsistent files or directories
$ ./bin/alluxio fs checkConsistency -r /
```

### checksum

The `checksum` command outputs the md5 value of a file in Alluxio.

For example, `checksum` can be used to verify the contents of a file stored in Alluxio.

```shell
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

```shell
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

```shell
$ ./bin/alluxio fs chmod 755 /input/file1
```

### chown

The `chown` command changes the owner of the file or directory in Alluxio.
For security reasons, the ownership of a file can only be altered by a super user.

For example, `chown` can be used as a quick way to change the owner of file:

```shell
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

```shell
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

```shell
$ ./bin/alluxio fs copyToLocal /output/part-00000 part-00000
$ wc -l part-00000
```

### distributedCp

The `distributedCp` command copies a file or directory in the Alluxio file system distributed across workers
using the job service. By default, the command runs synchronously and the user will get a `JOB_CONTROL_ID` after the command successfully submits the job to be executed.
The command will wait until the job is complete, at which point the user will see the list of files copied and statistics on which files completed or failed.
The command can also run in async mode with the `--async` flag. Similar to before, the user will get a `JOB_CONTROL_ID` after the command successfully submits the job.
The difference is that the command will not wait for the job to finish. 
Users can use the [`getCmdStatus`](#getCmdStatus) command with the `JOB_CONTROL_ID` as an argument to check detailed status information about the job.

If the source designates a directory, `distributedCp` copies the entire subtree at source to the destination.

**Options:**
* `--active-jobs`: Limits how many jobs can be submitted to the Alluxio job service at the same time.
Later jobs must wait until some earlier jobs to finish. The default value is `3000`.
A lower value means slower execution but also being nicer to the other users of the job service.
* `--overwrite`: Whether to overwrite the destination. Default is true.
* `--batch-size`: Specifies how many files to be batched into one request. The default value is `20`. Notice that if some task failed in the batched job, the whole batched job would fail with some completed tasks and some failed tasks.
* `--async`: Specifies whether to wait for command execution to finish. If not explicitly shown then default to run synchronously.

```shell
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

Turn on async submission mode. Run this command to get JOB_CONTROL_ID, then use getCmdStatus to check command detailed status:
```shell
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

**Options**

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

```shell
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
Turn on async submission mode. Run this command to get JOB_CONTROL_ID, then use getCmdStatus to check command detailed status:
```shell
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

```shell
# Only include host1 and host2
$ ./bin/alluxio fs distributedLoad /data/today --hosts host1,host2
```

```shell
# Only include the workset from host file /tmp/hostfile
$ ./bin/alluxio fs distributedLoad /data/today --host-file /tmp/hostfile
```

```shell
# Include all workers except host1 and host2
$ ./bin/alluxio fs distributedLoad /data/today --excluded-hosts host1,host2
```

```shell
# Include all workers except the workerset in the excluded host file /tmp/hostfile-exclude
$ ./bin/alluxio fs distributedLoad /data/today --excluded-file /tmp/hostfile-exclude
```

```shell
# Include workers which's locality identify belong to ROCK1 or ROCK2
$ ./bin/alluxio fs distributedLoad /data/today --locality ROCK1,ROCK2
```

```shell
# Include workers which's locality identify belong to the localities in the locality file
$ ./bin/alluxio fs distributedLoad /data/today --locality-file /tmp/localityfile
```

```shell
# Include all workers except which's locality belong to ROCK1 or ROCK2
$ ./bin/alluxio fs distributedLoad /data/today --excluded-locality ROCK1,ROCK2
```

```shell
# Include all workers except which's locality belong to the localities in the excluded locality file
$ ./bin/alluxio fs distributedLoad /data/today --excluded-locality-file /tmp/localityfile-exclude
```

**Conflict Cases:**

* The `--hosts` and `--locality` are `OR` relationship, so host2,host3 and workers in ROCK2,ROCKS3 will be included:
```shell
$ ./bin/alluxio fs distributedLoad /data/today --locality ROCK2,ROCK3 --hosts host2,host3
```

* The `--excluded-hosts` and `--excluded-locality` are `OR` relationship, so host2,host3 and workers in ROCK2,ROCKS3 will be excluded:
```shell
$ ./bin/alluxio fs distributedLoad /data/today --excluded-hosts host2,host3 --excluded-locality ROCK2,ROCK3
```

### distributedMv

The `distributedMv` command moves a file or directory in the Alluxio file system distributed across workers
using the job service.

If the source designates a directory, `distributedMv` moves the entire subtree at source to the destination.

```shell
$ ./bin/alluxio fs distributedMv /data/1023 /data/1024
```

### head

The `head` command prints the first 1 KB of data in a file to the shell.

Using the `-c [bytes]` option will print the first `n` bytes of data to the shell.

```shell
$ ./bin/alluxio fs head -c 2048 /output/part-00000
```

### help

The `help` command prints the help message for a given `fs` subcommand.
If the given command does not exist, it prints help messages for all supported subcommands.

Examples:
```shell
# Print all subcommands
$ ./bin/alluxio fs help
```

```shell
# Print help message for ls
$ ./bin/alluxio fs help ls
```

### leader

The `leader` command prints the current Alluxio leading master hostname.

```shell
$ ./bin/alluxio fs leader
```

### load

The `load` command moves data from the under storage system into Alluxio storage.
For example, `load` can be used to prefetch data for analytics jobs.
If `load` is run on a directory, files in the directory will be recursively loaded.
```shell
$ ./bin/alluxio fs load <path> --submit [--bandwidth N] [--verify] [--partial-listing]
```
**Options:**
* `--bandwidth` option specify how much ufs bandwidth we want to use to load files.
* `--verify` option specify whether we want to verify that all the files are loaded.
* `--partial-listing` option specify using batch listStatus API or traditional listStatus. We would retire this option when batch listStatus API gets mature.

After submit the command, you can check the status by running the following
```shell
$ ./bin/alluxio fs load <path> --progress [--format TEXT|JSON] [--verbose]
```
And you would get the following output:
```shell
Progress for loading path '/dir-99':
        Settings:       bandwidth: unlimited    verify: false
        Job State: SUCCEEDED
        Files Processed: 1000
        Bytes Loaded: 125.00MB
        Throughput: 2509.80KB/s
        Block load failure rate: 0.00%
        Files Failed: 0
```
**Options:**
* `--format` option specify output format. TEXT as default
* `--verbose` option output job details. 

```shell
# If you want to stop the command, run the following
$ ./bin/alluxio fs load <path> --stop
```

```shell
# If you just want sequential execution for couple files, you can use the following old version
$ ./bin/alluxio fs load <path>
```
If there is a Alluxio worker on the machine this command is run from, the data will be loaded to that worker.
Otherwise, a random worker will be selected to serve the data.

If the data is already loaded into Alluxio, load is a no-op unless the `--local flag` is used.
The `--local` flag forces the data to be loaded to a local worker
even if the data is already available on a remote worker.
```shell
$ ./bin/alluxio fs load <path> --local
```

### loadMetadata

The `loadMetadata` command loads metadata about a path in the UFS to Alluxio.
No data will be transferred.
This command is a client-side optimization without storing all returned `ls` results, preventing OOM for massive amount of small files.
This is useful when data has been added to the UFS outside of Alluxio and users are expected to reference the new data.
This command is more efficient than using the `ls` command since it does not store any directory or file information to be returned.

**Options:**
* `-R` option recursively loads metadata in subdirectories
* `-F` option updates the metadata of the existing file forcibly

For example, `loadMetadata` can be used to load metadata for a path in the UFS.
The -F option will force the loading of metadata even if there are existing metadata entries for the path.
```shell
$ ./bin/alluxio fs loadMetadata -R -F <path>
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

**Options:**

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
* `-m` option excludes mount point related information.

For example, `ls` can be used to browse the file system.

```shell
$ ./bin/alluxio fs mount /s3/data s3://data-bucket/
```

```shell
# Loads metadata for all immediate children of /s3/data and lists them
$ ./bin/alluxio fs ls /s3/data/
```

```shell
# Forces loading metadata
$ aws s3 cp /tmp/somedata s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
```

```shell
# Files are not removed from Alluxio if they are removed from the UFS (s3 here) only
$ aws s3 rm s3://data-bucket/somedata
$ ./bin/alluxio fs ls -f /s3/data
```

Metadata sync is an expensive operation. A rough estimation is metadata sync
on 1 million files will consume 2GB heap until the sync operation is complete.
Therefore, we recommend not using forced sync to avoid accidental repeated sync operations.
It is recommended to always specify a non-zero sync interval for metadata sync, so
even if the sync is repeatedly triggered, the paths that have just been sync-ed can be identified and skipped. 

```shell
# Should be avoided
$ ./bin/alluxio fs ls -f -R /s3/data
```
```shell
# RECOMMENDED. This will not sync files repeatedly in 1 minute.
$ ./bin/alluxio fs ls -Dalluxio.user.file.metadata.sync.interval=1min -R /s3/data
```

### masterInfo

The `masterInfo` command prints information regarding master fault tolerance such as leader address,
list of master addresses, and the configured Zookeeper address.
If Alluxio is running in single master mode, `masterInfo` prints the master address.
If Alluxio is running in fault tolerance mode, the leader address, list of master addresses
and the configured Zookeeper address is printed.

For example, `masterInfo` can be used to print information regarding master fault tolerance.

```shell
$ ./bin/alluxio fs masterInfo
```

### mkdir

The `mkdir` command creates a new directory in Alluxio space.
It is recursive and will create any nonexistent parent directories.
Note that the created directory will not be created in the under storage system
until a file in the directory is persisted to the underlying storage.
Using `mkdir` on an invalid or existing path will fail.

For example, `mkdir` can be used by an admin to set up the basic folder structures.

```shell
$ ./bin/alluxio fs mkdir /users
$ ./bin/alluxio fs mkdir /users/Alice
$ ./bin/alluxio fs mkdir /users/Bob
```

### mv

The `mv` command moves a file or directory to another path in Alluxio.
The destination path must not exist or be a directory.
If it is a directory, the file or directory will be placed as a child of the directory.
`mv` is purely a metadata operation and does not affect the data blocks of the file.
`mv` cannot be done between mount points of different under storage systems.

For example, `mv` can be used to re-organize your files.

```shell
$ ./bin/alluxio fs mv /data/2014 /data/archives/2014
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

```shell
# Remove a file from Alluxio space and the under storage system
$ ./bin/alluxio fs rm /tmp/unused-file
```

```shell
# Remove a file from Alluxio space only
$ ./bin/alluxio fs rm --alluxioOnly /tmp/unused-file2
```

When deleting only from Alluxio but leaving the files in UFS, we recommend using `-U` and `-Dalluxio.user.file.metadata.sync.interval=-1`
to skip the metadata sync and the UFS check. This will save time and memory consumption on the Alluxio master.
```shell
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
```shell
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


### stat

The `stat` command dumps the FileInfo representation of a file or a directory to the shell.
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
* `%i`: file ID(inode ID) of the file

For example, `stat` can be used to debug the block locations of a file.
This is useful when trying to achieve locality for compute workloads.

```shell
# Display file's stat
$ ./bin/alluxio fs stat /data/2015/logs-1.txt
```

```shell
# Display directory's stat
$ ./bin/alluxio fs stat /data/2015
```

```shell
# Display the size of file
$ ./bin/alluxio fs stat -f %z /data/2015/logs-1.txt
```

```shell
# Find the file by fileID/inodeID and display the stat, useful in troubleshooting
$ ./bin/alluxio fs stat -fileId 12345678
```

### tail

The `tail` command outputs the last 1 KB of data in a file to the shell.
Using the `-c [bytes]` option will print the last `n` bytes of data to the shell.

For example, `tail` can be used to verify the output of a job is in the expected format
or contains expected values.

```shell
$ ./bin/alluxio fs tail /output/part-00000
```
