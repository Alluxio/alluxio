---
layout: global
title: User Command Line Interface
---

{% comment %}
This is a generated file created by running command "bin/alluxio generate user-cli"
The command parses the golang command definitions and descriptions to generate the markdown in this file
{% endcomment %}

Alluxio's command line interface provides user access to various operations, such as:
- Start or stop processes
- Filesystem operations
- Administrative commands

Invoke the executable to view the possible subcommands:
```shell
$ ./bin/alluxio
Usage:
  bin/alluxio [command]

Available Commands:
  cache       Worker-related file system and format operations.
  conf        Get, set, and validate configuration settings, primarily those defined in conf/alluxio-site.properties
  exec        Run the main method of an Alluxio class, or end-to-end tests on an Alluxio cluster.
  fs          Operations to interface with the Alluxio filesystem
  generate    Generate files used in documentation
  help        Help about any command
  info        Retrieve and/or display info about the running Alluxio cluster
  init        Initialization operations such as format and validate
  job         Command line tool for interacting with the job service.
  journal     Journal related operations
  process     Start or stop cluster processes

Flags:
      --debug-log   True to enable debug logging

Use "bin/alluxio [command] --help" for more information about a command.

```

To set JVM system properties as part of the command, set the `-D` flag in the form of `-Dproperty=value`.

To attach debugging java options specified by `$ALLUXIO_USER_ATTACH_OPTS`, set the `--attach-debug` flag

Note that, as a part of Alluxio deployment, the Alluxio shell will also take the configuration in `${ALLUXIO_HOME}/conf/alluxio-site.properties` when it is run from Alluxio installation at `${ALLUXIO_HOME}`.

## cache
Worker-related file system and format operations.

### cache format
Usage: `bin/alluxio cache format`

The format command formats the Alluxio worker on this host.
This deletes all the cached data stored by the worker. Data in the under storage will not be changed.

> Warning: Format should only be called when the worker is not running

Examples:
```shell
# Format worker
$ ./bin/alluxio cache format
```


### cache free
Usage: `bin/alluxio cache free [flags]`

Synchronously free cached files along a path or held by a specific worker

Flags:
- `--path`: The file or directory to free (Default: "")
- `--worker`: The worker to free (Default: "")

Examples:
```shell
# Free a file by its path
$ ./bin/alluxio cache free --path /path/to/file
```

```shell
# Free files on a worker
$ ./bin/alluxio cache free --worker <workerHostName>
```


## conf
Get, set, and validate configuration settings, primarily those defined in conf/alluxio-site.properties

### conf get
Usage: `bin/alluxio conf get [key] [flags]`

The get command prints the configured value for the given key.
If the key is invalid, it returns a nonzero exit code.
If the key is valid but isn't set, an empty string is printed.
If no key is specified, the full configuration is printed.

> Note: This command does not require the Alluxio cluster to be running.

Flags:
- `--master`: Show configuration properties used by the master (Default: false)
- `--source`: Show source of the configuration property instead of the value (Default: false)
- `--unit`: Unit of the value to return, converted to correspond to the given unit.
E.g., with "--unit KB", a configuration value of "4096B" will return 4
Possible options include B, KB, MB, GB, TP, PB, MS, S, M, H, D (Default: "")

Examples:
```shell
# Display all the current node configuration
$ ./bin/alluxio conf get
```

```shell
# Display the value of a property key
$ ./bin/alluxio conf get alluxio.master.hostname
```

```shell
# Display the configuration of the current running Alluxio leading master
$ ./bin/alluxio conf get --master
```

```shell
# Display the source of the configuration
$ ./bin/alluxio conf get --source
```

```shell
# Display the values in a given unit
$ ./bin/alluxio conf get alluxio.user.block.size.bytes.default --unit KB
$ ./bin/alluxio conf get alluxio.master.journal.flush.timeout --unit S

```


### conf log
Usage: `bin/alluxio conf log [flags]`

The log command returns the current value of or updates the log level of a particular class on specific instances.
Users are able to change Alluxio server-side log levels at runtime.

The --target flag specifies which processes to apply the log level change to.
The target could be of the form <master|workers|job_master|job_workers|host:webPort[:role]> and multiple targets can be listed as comma-separated entries.
The role can be one of master,worker,job_master,job_worker.
Using the role option is useful when an Alluxio process is configured to use a non-standard web port (e.g. if an Alluxio master does not use 19999 as its web port).
The default target value is the primary master, primary job master, all workers and job workers.

> Note: This command requires the Alluxio cluster to be running.

Flags:
- `--level`: If specified, sets the specified logger at the given level (Default: "")
- `--name`: (Required) Logger name (ex. alluxio.master.file.DefaultFileSystemMaster)
- `--target`: A target name among <master|workers|job_master|job_workers|host:webPort[:role]>. Defaults to master,workers,job_master,job_workers (Default: [])

Examples:
```shell
# Set DEBUG level for DefaultFileSystemMaster class on master processes
$ ./bin/alluxio conf log --logName alluxio.master.file.DefaultFileSystemMaster --target=master --level=DEBUG
```

```shell
# Set WARN level for PagedDoraWorker class on the worker process on host myHostName
$ ./bin/alluxio conf log --logName alluxio.worker.dora.PagedDoraWorker.java --target=myHostName:worker --level=WARN

```


## exec
Run the main method of an Alluxio class, or end-to-end tests on an Alluxio cluster.

### exec basicIOTest
Usage: `bin/alluxio exec basicIOTest [flags]`

Run all end-to-end tests or a specific test, on an Alluxio cluster.

Flags:
- `--directory`: Alluxio path for the tests working directory. Default: / (Default: "")
- `--operation`: The operation to test, either BASIC or BASIC_NON_BYTE_BUFFER. 
By default both operations are tested. (Default: "")
- `--readType`: The read type to use, one of NO_CACHE, CACHE, CACHE_PROMOTE. 
By default all readTypes are tested. (Default: "")
- `--workers`: Alluxio worker addresses to run tests on. 
If not specified, random ones will be used. (Default: "")
- `--writeType`: The write type to use, one of MUST_CACHE, CACHE_THROUGH, THROUGH. 
By default all writeTypes are tested. (Default: "")

Examples:
```shell
# Run all permutations of IO tests
$ ./bin/alluxio exec basicIOTest
```

```shell
# Run a specific permutation of the IO tests
$ ./bin/alluxio exec basicIOtest --operation BASIC --readType NO_CACHE --writeType THROUGH
```


### exec class
Usage: `bin/alluxio exec class [flags]`

Run the main method of an Alluxio class.

Flags:
- `--jar`: Determine a JAR file to run. (Default: "")
- `--m`: Determine a module to run. (Default: "")

### exec hdfsMountTest
Usage: `bin/alluxio exec hdfsMountTest [flags]`

Tests runs a set of validations against the given hdfs path.

Flags:
- `--option`: options associated with this mount point. (Default: "")
- `--path`: (Required) specifies the HDFS path you want to validate.
- `--readonly`: mount point is readonly in Alluxio. (Default: false)
- `--shared`: mount point is shared. (Default: false)

### exec ufsIOTest
Usage: `bin/alluxio exec ufsIOTest [flags]`

A benchmarking tool for the I/O between Alluxio and UFS.
This test will measure the I/O throughput between Alluxio workers and the specified UFS path.
Each worker will create concurrent clients to first generate test files of the specified size then read those files.
The write/read I/O throughput will be measured in the process.

Flags:
- `--cluster`: specifies the benchmark is run in the Alluxio cluster.
If not specified, this benchmark will run locally. (Default: false)
- `--cluster-limit`: specifies how many Alluxio workers to run the benchmark concurrently.
If >0, it will only run on that number of workers.
If 0, it will run on all available cluster workers.
If <0, will run on the workers from the end of the worker list.
This flag is only used if --cluster is enabled. (Default: 0)
- `--io-size`: specifies the amount of data each thread writes/reads. (Default: "")
- `--java-opt`: The java options to add to the command line to for the task.
This can be repeated. The options must be quoted and prefixed with a space.
For example: --java-opt " -Xmx4g" --java-opt " -Xms2g". (Default: [])
- `--path`: (Required) specifies the path to write/read temporary data in.
- `--threads`: specifies the number of threads to concurrently use on each worker. (Default: 4)

Examples:
```shell
# This runs the I/O benchmark to HDFS in your process locally
$ ./bin/alluxio runUfsIOTest --path hdfs://<hdfs-address>
```

```shell
# This invokes the I/O benchmark to HDFS in the Alluxio cluster
# 1 worker will be used. 4 threads will be created, each writing then reading 4G of data
$ ./bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --cluster --cluster-limit 1
```

```shell
# This invokes the I/O benchmark to HDFS in the Alluxio cluster
# 2 workers will be used
# 2 threads will be created on each worker
# Each thread is writing then reading 512m of data
$ ./bin/alluxio runUfsIOTest --path hdfs://<hdfs-address> --cluster --cluster-limit 2 --io-size 512m --threads 2
```


### exec ufsTest
Usage: `bin/alluxio exec ufsTest [flags]`

Test the integration between Alluxio and the given UFS to validate UFS semantics

Flags:
- `--path`: (Required) the full UFS path to run tests against.
- `--test`: Test name, this option can be passed multiple times to indicate multipleZ tests (Default: [])

## fs
Operations to interface with the Alluxio filesystem
For commands that take Alluxio URIs as an argument such as ls or mkdir, the argument should be either
- A complete Alluxio URI, such as alluxio://<masterHostname>:<masterPort>/<path>
- A path without its scheme header, such as /path, in order to use the default hostname and port set in alluxio-site.properties

> Note: All fs commands require the Alluxio cluster to be running.

Most of the commands which require path components allow wildcard arguments for ease of use.
For example, the command "bin/alluxio fs rm '/data/2014*'" deletes anything in the data directory with a prefix of 2014.

Some shells will attempt to glob the input paths, causing strange errors.
As a workaround, you can disable globbing (depending on the shell type; for example, set -f) or by escaping wildcards
For example, the command "bin/alluxio fs cat /\\*" uses the escape backslash character twice.
This is because the shell script will eventually call a java program which should have the final escaped parameters "cat /\\*".


### fs cat
Usage: `bin/alluxio fs cat [path]`

The cat command prints the contents of a file in Alluxio to the shell.

Examples:
```shell
# Print the contents of /output/part-00000
$ ./bin/alluxio fs cat /output/part-00000
```


### fs check-cached
Usage: `bin/alluxio fs check-cached [path] [flags]`

Checks if files under a path have been cached in alluxio.

Flags:
- `--limit`: Limit number of files to check (Default: 1000)
- `--sample`: Sample ratio, 10 means sample 1 in every 10 files. (Default: 1)

### fs checksum
Usage: `bin/alluxio fs checksum [path]`

The checksum command outputs the md5 value of a file in Alluxio.
This can be used to verify the contents of a file stored in Alluxio.

Examples:
```shell
# Compare the checksum values
# value from Alluxio filesystem
$ ./bin/alluxio fs checksum /LICENSE
md5sum: bf0513403ff54711966f39b058e059a3
# value from local filesystem
md5 LICENSE
MD5 (LICENSE) = bf0513403ff54711966f39b058e059a3
```


### fs chgrp
Usage: `bin/alluxio fs chgrp [group] [path] [flags]`

The chgrp command changes the group of the file or directory in Alluxio.
Alluxio supports file authorization with POSIX file permissions.
The file owner or superuser can execute this command.

Flags:
- `--recursive`,`-R`: change the group recursively for all files and directories under the given path (Default: false)

Examples:
```shell
# Change the group of a file
$ ./bin/alluxio fs chgrp alluxio-group-new /input/file1
```


### fs chmod
Usage: `bin/alluxio fs chmod [mode] [path] [flags]`

The chmod command changes the permission of a file or directory in Alluxio.
The permission mode is represented as an octal 3 digit value.
Refer to https://en.wikipedia.org/wiki/Chmod#Numerical_permissions for a detailed description of the modes.

Flags:
- `--recursive`,`-R`: change the permission recursively for all files and directories under the given path (Default: false)

Examples:
```shell
# Set mode 755 for /input/file
$ ./bin/alluxio fs chmod 755 /input/file1
```


### fs chown
Usage: `bin/alluxio fs chown <owner>[:<group>] <path> [flags]`

The chown command changes the owner of a file or directory in Alluxio.
The ownership of a file can only be altered by a superuser

Flags:
- `--recursive`,`-R`: change the owner recursively for all files and directories under the given path (Default: false)

Examples:
```shell
# Change the owner of /input/file1 to alluxio-user
$ ./bin/alluxio fs chown alluxio-user /input/file1
```


### fs consistent-hash
Usage: `bin/alluxio fs consistent-hash [--create]|[--compare <1stCheckFilePath> <2ndCheckFilePath>]|[--clean] [flags]`

This command is for checking whether the consistent hash ring is changed or not

Flags:
- `--clean`: Delete generated check data (Default: false)
- `--compare`: Compare check files to see if the hash ring has changed (Default: false)
- `--create`: Generate check file (Default: false)

### fs cp
Usage: `bin/alluxio fs cp [srcPath] [dstPath] [flags]`

Copies a file or directory in the Alluxio filesystem or between local and Alluxio filesystems.
The file:// scheme indicates a local filesystem path and the alluxio:// scheme or no scheme indicates an Alluxio filesystem path.

Flags:
- `--buffer-size`: Read buffer size when coping to or from local, with defaults of 64MB and 8MB respectively (Default: "")
- `--preserve`,`-p`: Preserve file permission attributes when copying files; all ownership, permissions, and ACLs will be preserved (Default: false)
- `--recursive`,`-R`: True to copy the directory subtree to the destination directory (Default: false)
- `--thread`: Number of threads used to copy files in parallel, defaults to 2 * CPU cores (Default: 0)

Examples:
```shell
# Copy within the Alluxio filesystem
$ ./bin/alluxio fs cp /file1 /file2
```

```shell
# Copy a local file to the Alluxio filesystem
$ ./bin/alluxio fs cp file:///file1 /file2
```

```shell
# Copy a file in Alluxio to local
$ ./bin/alluxio fs cp alluxio:///file1 file:///file2
```

```shell
# Recursively copy a directory within the Alluxio filesystem
$ ./bin/alluxio fs cp -R /dir1 /dir2

```


### fs head
Usage: `bin/alluxio fs head [path] [flags]`

The head command prints the first 1KB of data of a file to the shell.
Specifying the -c flag sets the number of bytes to print.

Flags:
- `--bytes`,`-c`: Byte size to print (Default: "")

Examples:
```shell
# Print first 2048 bytes of a file
$ ./bin/alluxio fs head -c 2048 /output/part-00000
```


### fs location
Usage: `bin/alluxio fs location [path]`

Displays the list of hosts storing the specified file.

### fs ls
Usage: `bin/alluxio fs ls [path] [flags]`

The ls command lists all the immediate children in a directory and displays the file size, last modification time, and in memory status of the files.
Using ls on a file will only display the information for that specific file.

The ls command will also load the metadata for any file or immediate children of a directory from the under storage system to Alluxio namespace if it does not exist in Alluxio.
It queries the under storage system for any file or directory matching the given path and creates a mirror of the file in Alluxio backed by that file.
Only the metadata, such as the file name and size, are loaded this way and no data transfer occurs.

Flags:
- `--help`: help for this command (Default: false)
- `--human-readable`,`-h`: Print sizes in human readable format (Default: false)
- `--list-dir-as-file`,`-d`: List directories as files (Default: false)
- `--load-metadata`,`-f`: Force load metadata for immediate children in a directory (Default: false)
- `--omit-mount-info`,`-m`: Omit mount point related information such as the UFS path (Default: false)
- `--pinned-files`,`-p`: Only show pinned files (Default: false)
- `--recursive`,`-R`: List subdirectories recursively (Default: false)
- `--reverse`,`-r`: Reverse sorted order (Default: false)
- `--sort`: Sort entries by column, one of {creationTime|inMemoryPercentage|lastAccessTime|lastModificationTime|name|path|size} (Default: "")
- `--timestamp`: Display specified timestamp of entry, one of {createdTime|lastAccessTime|lastModifiedTime} (Default: "")

Examples:
```shell
# List and load metadata for all immediate children of /s3/data
$ ./bin/alluxio fs ls /s3/data
```

```shell
# Force loading metadata of /s3/data
$ ./bin/alluxio fs ls -f /s3/data
```


### fs mkdir
Usage: `bin/alluxio fs mkdir [path1 path2 ...]`

The mkdir command creates a new directory in the Alluxio filesystem.
It is recursive and will create any parent directories that do not exist.
Note that the created directory will not be created in the under storage system until a file in the directory is persisted to the underlying storage.
Using mkdir on an invalid or existing path will fail.

Examples:
```shell
# Creating a folder structure
$ ./bin/alluxio fs mkdir /users
$ ./bin/alluxio fs mkdir /users/Alice
$ ./bin/alluxio fs mkdir /users/Bob
```


### fs mv
Usage: `bin/alluxio fs mv [srcPath] [dstPath]`

The mv command moves a file or directory to another path in Alluxio.
The destination path must not exist or be a directory.
If it is a directory, the file or directory will be placed as a child of the directory.
The command is purely a metadata operation and does not affect the data blocks of the file.

Examples:
```shell
# Moving a file
$ ./bin/alluxio fs mv /data/2014 /data/archives/2014
```


### fs rm
Usage: `bin/alluxio fs rm [path] [flags]`

The rm command removes a file from Alluxio space and the under storage system.
The file will be unavailable immediately after this command returns, but the actual data may be deleted a while later.

Flags:
- `--alluxio-only`: True to only remove data and metadata from Alluxio cache (Default: false)
- `--recursive`,`-R`: True to recursively remove files within the specified directory subtree (Default: false)
- `--skip-ufs-check`,`-U`: True to skip checking if corresponding UFS contents are in sync (Default: false)

Examples:
```shell
# Remove a file from Alluxio and the under storage system
$ ./bin/alluxio fs rm /tmp/unused-file
```

```shell
# Remove a file from Alluxio filesystem only
$ ./bin/alluxio fs rm --alluxio-only --skip-ufs-check /tmp/unused-file2
# Note it is recommended to use both --alluxio-only and --skip-ufs-check together in this situation
```


### fs stat
Usage: `bin/alluxio fs stat [flags]`

The stat command dumps the FileInfo representation of a file or a directory to the shell.

Flags:
- `--file-id`: File id of file (Default: "")
- `--format`,`-f`: Display info in the given format:
  "%N": name of the file
  "%z": size of file in bytes
  "%u": owner
  "%g": group name of owner
  "%i": file id of the file
  "%y": modification time in UTC in 'yyyy-MM-dd HH:mm:ss' format
  "%Y": modification time as Unix timestamp in milliseconds
  "%b": Number of blocks allocated for file
 (Default: "")
- `--path`: Path to file or directory (Default: "")

Examples:
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
# Find the file by fileID and display the stat, useful in troubleshooting
$ ./bin/alluxio fs stat -fileId 12345678
```


### fs tail
Usage: `bin/alluxio fs tail [path] [flags]`

The tail command prints the last 1KB of data of a file to the shell.
Specifying the -c flag sets the number of bytes to print.

Flags:
- `--bytes`: Byte size to print (Default: "")

Examples:
```shell
# Print last 2048 bytes of a file
$ ./bin/alluxio fs tail -c 2048 /output/part-00000
```


### fs test
Usage: `bin/alluxio fs test [path] [flags]`

Test a property of a path, returning 0 if the property is true, or 1 otherwise

Flags:
- `--dir`,`-d`: Test if path is a directory (Default: false)
- `--exists`,`-e`: Test if path exists (Default: false)
- `--file`,`-f`: Test if path is a file (Default: false)
- `--not-empty`,`-s`: Test if path is not empty (Default: false)
- `--zero`,`-z`: Test if path is zero length (Default: false)

### fs touch
Usage: `bin/alluxio fs touch [path]`

Create a 0 byte file at the specified path, which will also be created in the under file system

## generate
Generate files used in documentation

### generate doc-tables
Usage: `bin/alluxio generate doc-tables`

Generate configuration and metric tables used in documentation

### generate docs
Usage: `bin/alluxio generate docs`

Generate all documentation files

### generate user-cli
Usage: `bin/alluxio generate user-cli [flags]`

Generate content for `operation/User-CLI.md`

Flags:
- `--help`,`-h`: help for user-cli (Default: false)

## info
Retrieve and/or display info about the running Alluxio cluster

### info cache
Usage: `bin/alluxio info cache [flags]`

Reports worker capacity information

Flags:
- `--live`: Only show live workers for capacity report (Default: false)
- `--lost`: Only show lost workers for capacity report (Default: false)
- `--worker`: Only show specified workers for capacity report, labeled by hostname or IP address (Default: [])

### info collect
Usage: `bin/alluxio info collect [command] [flags]`

Collects information such as logs, config, metrics, and more from the running Alluxio cluster and bundle into a single tarball

[command] must be one of the following values:
- all: runs all the commands below
- cluster: runs a set of Alluxio commands to collect information about the Alluxio cluster
- conf: collects the configuration files under ${ALLUXIO_HOME}/config/
- env: runs a set of linux commands to collect information about the cluster
- jvm: collects jstack from the JVMs
- log: collects the log files under ${ALLUXIO_HOME}/logs/
- metrics: collects Alluxio system metrics

> WARNING: This command MAY bundle credentials. Inspect the output tarball for any sensitive information and remove it before sharing with others.

Flags:
- `--additional-logs`: Additional file name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, inclusive of the default log files (Default: [])
- `--end-time`: Logs that do not contain entries before this time will be ignored, format must be like 2006-01-02T15:04:05 (Default: "")
- `--exclude-logs`: File name prefixes from ${ALLUXIO_HOME}/logs to exclude; this is evaluated after adding files from --additional-logs (Default: [])
- `--exclude-worker-metrics`: True to skip worker metrics collection (Default: false)
- `--include-logs`: File name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, ignoring the default log files; cannot be used with --exclude-logs or --additional-logs (Default: [])
- `--local`: True to only collect information from the local machine (Default: false)
- `--max-threads`: Parallelism of the command; use a smaller value to limit network I/O when transferring tarballs (Default: 1)
- `--output-dir`: (Required) Output directory to write collect info tarball to
- `--start-time`: Logs that do not contain entries after this time will be ignored, format must be like 2006-01-02T15:04:05 (Default: "")

### info doctor
Usage: `bin/alluxio info doctor [type]`

Runs doctor configuration or storage command

### info nodes
Usage: `bin/alluxio info nodes`

Show all registered workers' status

### info report
Usage: `bin/alluxio info report [arg] [flags]`

Reports Alluxio running cluster information
[arg] can be one of the following values:
  jobservice: job service metrics information
  metrics:    metrics information
  summary:    cluster summary
  ufs:        under storage system information

Defaults to summary if no arg is provided


Flags:
- `--format`: Set output format, any of [json, yaml] (Default: "")

### info version
Usage: `bin/alluxio info version`

Print Alluxio version.

## init
Initialization operations such as format and validate

### init clear-os-cache
Usage: `bin/alluxio init clear-os-cache`

The clear-os-cache command drops the OS buffer cache

### init copy-dir
Usage: `bin/alluxio init copy-dir [path]`

The copy-dir command copies the directory at given path to all master nodes listed in conf/masters and all worker nodes listed in conf/workers.

> Note: This command does not require the Alluxio cluster to be running.

Examples:
```shell
# copy alluxio-site properties file to all nodes
$ ./bin/alluxio init copy-dir conf/alluxio-site.properties
```


### init format
Usage: `bin/alluxio init format [flags]`

The format command formats the Alluxio master and all its workers.

Running this command on an existing Alluxio cluster deletes everything persisted in Alluxio, including cached data and any metadata information.
Data in under storage will not be changed.

> Warning: Formatting is required when you run Alluxio for the first time.
It should only be called while the cluster is not running.


Flags:
- `--localFileSystem`,`-s`: Only format if underfs is local and doesn't already exist (Default: false)
- `--skip-master`: Skip formatting journal on all masters (Default: false)
- `--skip-worker`: Skip formatting cache on all workers (Default: false)

### init validate
Usage: `bin/alluxio init validate [flags]`

Validate Alluxio configuration or environment

Flags:
- `--type`: Decide the type to validate. Valid inputs: [conf, env] (Default: "")

Examples:
```shell
# Validate configuration
$ ./bin/alluxio init validate --type conf
```

```shell
# Validate environment
$ ./bin/alluxio init validate --type env
```


## job
Command line tool for interacting with the job service.

### job load
Usage: `bin/alluxio job load [flags]`

The load command moves data from the under storage system into Alluxio storage.
For example, load can be used to prefetch data for analytics jobs.
If load is run on a directory, files in the directory will be recursively loaded.

Flags:
- `--bandwidth`: [submit] Single worker read bandwidth limit (Default: "")
- `--format`: [progress] Format of output, either TEXT or JSON (Default: "")
- `--metadata-only`: [submit] Only load file metadata (Default: false)
- `--partial-listing`: [submit] Use partial directory listing, initializing load before reading the entire directory but cannot report on certain progress details (Default: false)
- `--path`: (Required) [all] Source path of load operation
- `--progress`: View progress of submitted job (Default: false)
- `--skip-if-exists`: [submit] Skip existing fullly cached files (Default: false)
- `--stop`: Stop running job (Default: false)
- `--submit`: Submit job (Default: false)
- `--verbose`: [progress] Verbose output (Default: false)
- `--verify`: [submit] Run verification when load finishes and load new files if any (Default: false)

Examples:
```shell
# Submit a load job
$ ./bin/alluxio job load --path /path --submit
```

```shell
# View the progress of a submitted job
$ ./bin/alluxio job load --path /path --progress
# Example output
Progress for loading path '/path':
        Settings:       bandwidth: unlimited    verify: false
        Job State: SUCCEEDED
        Files Processed: 1000
        Bytes Loaded: 125.00MB
        Throughput: 2509.80KB/s
        Block load failure rate: 0.00%
        Files Failed: 0
```

```shell
# Stop a submitted job
$ ./bin/alluxio job load --path /path --stop
```


## journal
Journal related operations

### journal checkpoint
Usage: `bin/alluxio journal checkpoint`

The checkpoint command creates a checkpoint the leading Alluxio master's journal.
This command is mainly used for debugging and to avoid master journal logs from growing unbounded.
Checkpointing requires a pause in master metadata changes, so use this command sparingly to avoid interfering with other users of the system.

### journal format
Usage: `bin/alluxio journal format`

The format command formats the local Alluxio master's journal.

> Warning: Formatting should only be called while the cluster is not running.

### journal read
Usage: `bin/alluxio journal read [flags]`

The read command parses the current journal and outputs a human readable version to the local folder.
This command may take a while depending on the size of the journal.
> Note: This command requies that the Alluxio cluster is NOT running.

Flags:
- `--end`: end log sequence number (exclusive) (Default: -1)
- `--input-dir`: input directory on-disk to read the journal content from (Default: "")
- `--master`: name of the master class (Default: "")
- `--output-dir`: output directory to write journal content to (Default: "")
- `--start`: start log sequence number (inclusive) (Default: 0)

Examples:
```shell
$ ./bin/alluxio readJournal
# output
Dumping journal of type EMBEDDED to /Users/alluxio/journal_dump-1602698211916
2020-10-14 10:56:51,960 INFO  RaftStorageDirectory - Lock on /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/in_use.lock acquired by nodename 78602@alluxio-user
2020-10-14 10:56:52,254 INFO  RaftJournalDumper - Read 223 entries from log /Users/alluxio/alluxio/journal/raft/02511d47-d67c-49a3-9011-abb3109a44c1/current/log_0-222.
```


## process
Start or stop cluster processes

### process start
Usage: `bin/alluxio process start [flags]`

Starts a single process locally or a group of similar processes across the cluster.
For starting a group, it is assumed the local host has passwordless SSH access to other nodes in the cluster.
The command will parse the hostnames to run on by reading the conf/masters and conf/workers files, depending on the process type.

Flags:
- `--async`,`-a`: Asynchronously start processes without monitoring for start completion (Default: false)
- `--skip-kill-prev`,`-N`: Avoid killing previous running processes when starting (Default: false)

### process stop
Usage: `bin/alluxio process stop [flags]`

Stops a single process locally or a group of similar processes across the cluster.
For stopping a group, it is assumed the local host has passwordless SSH access to other nodes in the cluster.
The command will parse the hostnames to run on by reading the conf/masters and conf/workers files, depending on the process type.

Flags:
- `--soft`,`-s`: Soft kill only, don't forcibly kill the process (Default: false)

