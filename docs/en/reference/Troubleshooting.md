---
layout: global
title: Troubleshooting
---


This page is a collection of high-level guides and tips regarding how to diagnose issues encountered in
Alluxio.

> Note: this doc is not intended to be the full list of Alluxio questions.
Join the [Alluxio community Slack Channel](https://www.alluxio.io/slack) to chat with users and
developers, or post questions on [Github issues](https://github.com/Alluxio/alluxio/issues){:target="_blank"}.

## Where are the Alluxio logs?

Alluxio generates Master, Worker and Client logs under the dir `${ALLUXIO_HOME}/logs`. See [Logging]({{ '/en/operation/Logging.html' | relativize_url }}) for more information. You can find details about [Server Logs]({{ '/en/operation/Logging.html#server-logs' | relativize_url }}) and [Application Logs]({{ '/en/operation/Logging.html#application-logs' | relativize_url }}).

## Alluxio remote debug

### Debugging Alluxio processes

Java remote debugging makes it easier to debug Alluxio at the source level without modifying any code. You
will need to set the JVM remote debugging parameters before starting the process. There are several ways to add
the remote debugging parameters; you can export the following configuration properties in shell or `conf/alluxio-env.sh`:

```shell
# Java 8
export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60001"
export ALLUXIO_WORKER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60002"
```

```shell
# Java 11
export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60001"
export ALLUXIO_WORKER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60002"
```

In general, you can use `ALLUXIO_<PROCESS>_ATTACH_OPTS` to specify how an Alluxio process should be attached to.

`suspend={y | n}` will decide whether the JVM process waits until the debugger connects or not.

`address` determines which port the Alluxio process will use to be attached to by a debugger. If left blank, it will
choose an open port by itself.

After completing this setup, learn how [to attach](#to-attach).

### Debugging shell commands

If you want to debug shell commands (e.g. `bin/alluxio fs ls /`), you can set the `ALLUXIO_USER_ATTACH_OPTS` in
`conf/alluxio-env.sh` as above:

```shell
# Java 8
export ALLUXIO_USER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60000"
```

```shell
# Java 11
export ALLUXIO_USER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60000"
```

After setting this parameter, you can add the `-debug` flag to start a debug server such as `bin/alluxio fs ls / --attach-debug`.

After completing this setup, learn how [to attach](#to-attach).

### To attach

There exists a [comprehensive tutorial on how to attach to and debug a Java process in IntelliJ](https://www.jetbrains.com/help/idea/attaching-to-local-process.html){:target="_blank"}.

Start the process or shell command of interest, then create a new java remote configuration,
set the debug server's host and port, and start the debug session. If you set a breakpoint which can be reached, the IDE
will enter debug mode. You can inspect the current context's variables, call stack, thread list, and expression
evaluation.

## Alluxio collectInfo command

Alluxio has a `collectInfo` command that collect information to troubleshoot an Alluxio cluster.
`collectInfo` will run a set of sub-commands that each collects one aspect of system information, as explained below.
In the end the collected information will be bundled into one tarball which contains a lot of information regarding your Alluxio cluster.
The tarball size mostly depends on your cluster size and how much information you are collecting.
For example, `collectLog` operation can be costly if you have huge amounts of logs. Other commands
typically do not generate files larger than 1MB. The information in the tarball will help you troubleshoot your cluster.
Or you can share the tarball with someone you trust to help troubleshoot your Alluxio cluster.

The `collectInfo` command will SSH to each node and execute the set of sub-commands.
In the end of execution the collected information will be written to files and tarballed.
Each individual tarball will be collected to the issuing node.
Then all the tarballs will be bundled into the final tarball, which contains all information about the Alluxio cluster.

> NOTE: Be careful if your configuration contains credentials like AWS keys!
You should ALWAYS CHECK what is in the tarball and REMOVE the sensitive information from the tarball before sharing it with someone!

### Collect Alluxio cluster information
`collectAlluxioInfo` will run a set of Alluxio commands that collect information about the Alluxio cluster, like `bin/alluxio info report` etc.
When the Alluxio cluster is not running, this command will fail to collect some information.
This sub-command will run both `bin/alluxio conf get` which collects local configuration properties,
and `bin/alluxio conf get --master --source` which prints configuration properties that are received from the master.
Both of them mask credential properties. The difference is the latter command fails if the Alluxio cluster is not up. 

### Collect Alluxio configuration files
`collectConfig` will collect all the configuration files under `${alluxio.work.dir}/conf`.
From Alluxio 2.4, the `alluxio-site.properties` file will not be copied,
as many users tend to put their plaintext credentials to the UFS in this file.
Instead, the `collectAlluxioInfo` will run a `alluxio conf get` command
which prints all the configuration properties, with the credential fields masked.
The [conf get command]({{ '/en/operation/User-CLI.html#conf-get' | relativize_url }}) will collect all the current node configuration.

So in order to collect Alluxio configuration in the tarball,
please make sure `collectAlluxioInfo` sub-command is run.

> WARNING: If you put credential fields in the configuration files except alluxio-site.properties (eg. `alluxio-env.sh`), 
DO NOT share the collected tarball with anybody unless you have manually obfuscated them in the tarball!

### Collect Alluxio logs
`collectLog` will collect all the logs under `${alluxio.work.dir}/logs`.

> NOTE: Roughly estimate how much log you are collecting before executing this command!

### Collect Alluxio metrics
`collectMetrics` will collect Alluxio metrics served at `http://${alluxio.master.hostname}:${alluxio.master.web.port}/metrics/json/` by default.
The metrics will be collected multiple times to see the progress.

### Collect JVM information
`collectJvmInfo` will collect information about the existing JVMs on each node.
This is done by running a `jps` command then `jstack` on each found JVM process.
This will be done multiple times to see if the JVMs are making progress.

### Collect system information
`collectEnv` will run a set of bash commands to collect information about the running node.
This runs system troubleshooting commands like `env`, `hostname`, `top`, `ps` etc.

> WARNING: If you stored credential fields in environment variables like AWS_ACCESS_KEY or in process start parameters
like `-Daws.access.key=XXX`, DO NOT share the collected tarball with anybody unless you have manually obfuscated them in the tarball!

### Collect all information mentioned above
`all` will run all the sub-commands above.

### Command options

The `collectInfo` command has the below options.

```shell
Collects information such as logs, config, metrics, and more from the running Alluxio cluster and bundle into a single tarball
[command] must be one of the following values:
  all      runs all the commands below
  cluster: runs a set of Alluxio commands to collect information about the Alluxio cluster
  conf:    collects the configuration files under ${ALLUXIO_HOME}/config/
  env:     runs a set of linux commands to collect information about the cluster
  jvm:     collects jstack from the JVMs
  log:     collects the log files under ${ALLUXIO_HOME}/logs/
  metrics: collects Alluxio system metrics

WARNING: This command MAY bundle credentials. To understand the risks refer to the docs here.
https://docs.alluxio.io/os/user/edge/en/operation/Troubleshooting.html#collect-alluxio-cluster-information

Usage:
  bin/alluxio info collect [command] [flags]

Flags:
      --additional-logs strings   Additional file name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, inclusive of the default log files
      --attach-debug              True to attach debug opts specified by $ALLUXIO_USER_ATTACH_OPTS
      --end-time string           Logs that do not contain entries before this time will be ignored, format must be like 2006-01-02T15:04:05
      --exclude-logs strings      File name prefixes from ${ALLUXIO_HOME}/logs to exclude; this is evaluated after adding files from --additional-logs
      --exclude-worker-metrics    True to skip worker metrics collection
  -h, --help                      help for collect
      --include-logs strings      File name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, ignoring the default log files; cannot be used with --exclude-logs or --additional-logs
  -D, --java-opts strings         Alluxio properties to apply, ex. -Dkey=value
      --local                     True to only collect information from the local machine
      --max-threads int           Parallelism of the command; use a smaller value to limit network I/O when transferring tarballs (default 1)
      --output-dir string         Output directory to write collect info tarball to
      --start-time string         Logs that do not contain entries after this time will be ignored, format must be like 2006-01-02T15:04:05

```

`--output-dir` is a required flag, specifying the directory to write the final tarball to.

Options:
1. `--max-threads threadNum` option configures how many threads to use for concurrently collecting information and transmitting tarballs.
When the cluster has a large number of nodes, or large log files, the network IO for transmitting tarballs can be significant.
Use this parameter to constrain the resource usage of this command.

1. `--local` option specifies the `collectInfo` command to run only on `localhost`.
That means the command will only collect information about the `localhost`. 
If your cluster does not have password-less SSH across nodes, you will need to run with `--local`
option locally on each node in the cluster, and manually gather all outputs.
If your cluster has password-less SSH across nodes, you can run without `--local` command,
which will essentially distribute the task to each node and gather the locally collected tarballs for you. 

1. `--help` option asks the command to print the help message and exit.

1. `--additional-logs <filename-prefixes>` specifies extra log file name prefixes to include.
By default, only log files recognized by Alluxio will be collected by the `collectInfo` command.
The recognized files include below:
```
logs/master.log*, 
logs/master.out*, 
logs/job_master.log*, 
logs/job_master.out*, 
logs/master_audit.log*, 
logs/worker.log*, 
logs/worker.out*, 
logs/job_worker.log*, 
logs/job_worker.out*, 
logs/proxy.log*, 
logs/proxy.out*, 
logs/task.log*, 
logs/task.out*, 
logs/user/*
```
Other than mentioned above, `--additional-logs <filename-prefixes>` specifies that files 
whose names start with the prefixes in `<filename-prefixes>` should be collected.
This will be checked after the exclusions defined in `--exclude-logs`.
`<filename-prefixes>`  specifies the filename prefixes, separated by commas.

1. `--exclude-logs <filename-prefixes>` specifies file name prefixes to ignore from the default list.

1. `--include-logs <filename-prefixes>` specifies only to collect files whose names start
with the specified prefixes, and ignore all the rest.
You CANNOT use `--include-logs` option together with either `--additional-logs` or
`--exclude-logs`, because it is ambiguous what you want to include.

1. `--end-time <datetime>` specifies a datetime after which the log files can be ignored.
A log file will be ignore if the file was created after this end time.
The first couple of lines of the log file will be parsed, in order to infer when the log
file started.
The `<datetime>` is a datetime string like `2020-06-27T11:58:53`.
The parsable datetime formats include below:
```
"2020-01-03 12:10:11,874"
"2020-01-03 12:10:11"
"2020-01-03 12:10"
"20/01/03 12:10:11"
"20/01/03 12:10"
2020-01-03T12:10:11.874+0800
2020-01-03T12:10:11
2020-01-03T12:10
```

1. `--start-time <datetime>` specifies a datetime before with the log files can be ignored.
A log file will be ignored if the last modified time is before this start time.

## Alluxio limitations

### File path limitations

There are some special characters and patterns in file path names that are not supported in Alluxio.
Please avoid creating file path names with these patterns or acquire additional handling from client end.
1. Question mark (`'?'`)
1. Pattern with period (`./` and `../`)
1. Backslash (`'\'`)

## Resource Leak Detection

If you are operating your Alluxio cluster it is possible you may notice a
message in the logs like:

```
LEAK: <>.close() was not called before resource is garbage-collected. See https://docs.alluxio.io/os/user/stable/en/administration/Troubleshooting.html#resource-leak-detection for more information about this message.
```

Alluxio has a built-in detection mechanism to help identify potential resource
leaks. This message indicates there is a bug in the Alluxio code which is
causing a resource leak. If  this message appears during cluster operation,
please [open a GitHub
Issue](https://github.com/Alluxio/alluxio/issues/new/choose){:target="_blank"} as a bug report and
share your log message and any relevant stack traces that are shared with it.

By default, Alluxio samples a portion of some resource allocations when
detecting these leaks, and for each tracked resource record the object's recent
accesses. The sampling rate and access tracking will result in a resource and
performance penalty. The amount of overhead introduced by the leak detector can
be controlled through the property `alluxio.leak.detector.level`. Valid values
are

- `DISABLED`: no leak tracking or logging is performed, lowest overhead
- `SIMPLE`: samples and tracks only leaks and does not log recent accesses. minimal overhead
- `ADVANCED`: samples and tracks recent accesses, higher overhead
- `PARANOID`: tracks for leaks on every resource allocation, highest overhead. 

## Master Internal Monitoring

Alluxio master periodically checks its resource usage, including CPU and memory usage, and several internal data structures 
that are performance critical. This interval is configured by `alluxio.master.throttle.heartbeat.interval` (defaults to 3 seconds).
On every sampling point in time (PIT), Alluxio master takes a snapshot of its resource usage. A continuous number of PIT snapshots
(number configured by alluxio.master.throttle.observed.pit.number, defaults to 3) will be saved and used to generate the aggregated
resource usage which is used to decide the system status.

Each PIT includes the following metrics.

```
directMemUsed=5268082, heapMax=59846950912, heapUsed=53165684872, cpuLoad=0.4453061982287778, pitTotalJVMPauseTimeMS=190107, totalJVMPauseTimeMS=0, rpcQueueSize=0, pitTimeMS=1665995384998}
```

- `directMemUsed`: direct memory allocated by `ByteBuffer.allocateDirect`
- `heapMax` : the allowed max heap size
- `heapUsed` : the heap memory used
- `cpuLoad` : the cpu load
- `pitTotalJVMPauseTimeMS` : aggregated total JVM pause time from the beginning
- `totalJVMPauseTimeMS` : the JVM pause time since last PIT
- `rpcQueueSize` : the rpc queue size
- `pitTimeMS` : the timestamp in millisecond when this snapshot is taken

The aggregated server indicators are the certain number of continuous PITs, this one is generated in a sliding window. The alluxio
master has a derived indicator `Master.system.status` that is based on the heuristic algorithm.

```
"Master.system.status" : {
  "value" : "STRESSED"
}
```

The possible statuses are: 
- `IDLE`
- `ACTIVE`
- `STRESSED`
- `OVERLOADED`

The system status is mainly decided by the JVM pause time and the free heap memory. Usually the status transition is 
`IDLE` <---> `ACTIVE` <---> `STRESSED` <---> `OVERLOADED`
1. If the JVM pause time is longer than `alluxio.master.throttle.overloaded.heap.gc.time`, the system status is directly set to `OVERLOADED`.
2. If the used heap memory is less than the low used heap memory boundary threshold, the system.status is deescalated.
3. If the used heap memory is less than the upper used heap memory boundary threshold, the system.status is unchanged.
4. If the aggregated used heap memory is greater than the upper used heap memory boundary threshold, the sytem.status is escalated.
5. As the used heap memory grows or shrinks, the value of the system status will update if it crosses any of the thresholds defined by the configurations below

The thresholds are
```properties
# JVM paused time
alluxio.master.throttle.overloaded.heap.gc.time
```

```properties
# heap used thresholds
alluxio.master.throttle.active.heap.used.ratio
alluxio.master.throttle.stressed.heap.used.ratio
alluxio.master.throttle.overloaded.heap.used.ratio
```

If the system status is `STRESSED` or `OVERLOADED`, `WARN` level log would be printed containing the following the filesystem indicators:
```
2022-10-17 08:29:41,998 WARN  SystemMonitor - System transition status is UNCHANGED, status is STRESSED, related Server aggregate indicators:ServerIndicator{directMemUsed=15804246, heapMax=58686177280, heapUsed=157767176816, cpuLoad=1.335918594686334, pitTotalJVMPauseTimeMS=62455, totalJVMPauseTimeMS=6, rpcQueueSize=0, pitTimeMS=1665989354196}, pit indicators:ServerIndicator{directMemUsed=5268082, heapMax=59846950912, heapUsed=48601091600, cpuLoad=0.4453061982287778, pitTotalJVMPauseTimeMS=190107, totalJVMPauseTimeMS=0, rpcQueueSize=0, pitTimeMS=1665995381998}
2022-10-17 08:29:41,998 WARN  SystemMonitor - The delta filesystem indicators FileSystemIndicator{Master.DeletePathOps=0, Master.PathsDeleted=0, Master.MetadataSyncPathsFail=0, Master.CreateFileOps=0, Master.ListingCacheHits=0, Master.MetadataSyncSkipped=3376, Master.UfsStatusCacheSize=0, Master.CreateDirectoryOps=0, Master.FileBlockInfosGot=0, Master.MetadataSyncPrefetchFail=0, Master.FilesCompleted=0, Master.RenamePathOps=0, Master.MetadataSyncSuccess=0, Master.MetadataSyncActivePaths=0, Master.FilesCreated=0, Master.PathsRenamed=0, Master.FilesPersisted=658, Master.CompletedOperationRetryCount=0, Master.ListingCacheEvictions=0, Master.MetadataSyncTimeMs=0, Master.SetAclOps=0, Master.PathsMounted=0, Master.FreeFileOps=0, Master.PathsUnmounted=0, Master.CompleteFileOps=0, Master.NewBlocksGot=0, Master.GetNewBlockOps=0, Master.ListingCacheMisses=0, Master.FileInfosGot=3376, Master.GetFileInfoOps=3376, Master.GetFileBlockInfoOps=0, Master.UnmountOps=0, Master.MetadataSyncPrefetchPaths=0, Master.getConfigHashInProgress=0, Master.MetadataSyncPathsSuccess=0, Master.FilesFreed=0, Master.MetadataSyncNoChange=0, Master.SetAttributeOps=0, Master.getConfigurationInProgress=0, Master.MetadataSyncPendingPaths=0, Master.DirectoriesCreated=0, Master.ListingCacheLoadTimes=0, Master.MetadataSyncPrefetchSuccess=0, Master.MountOps=0, Master.UfsStatusCacheChildrenSize=0, Master.MetadataSyncPrefetchOpsCount=0, Master.registerWorkerStartInProgress=0, Master.MetadataSyncPrefetchCancel=0, Master.MetadataSyncPathsCancel=0, Master.MetadataSyncPrefetchRetries=0, Master.MetadataSyncFail=0, Master.MetadataSyncOpsCount=3376}
```

The monitoring indicators describe the system status in a heuristic way to have a basic understanding of its load.
