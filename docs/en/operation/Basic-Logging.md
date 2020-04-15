---
layout: global
title: Basic Logging
nickname: Basic Logging
group: Operations
priority: 11
---

* Table of Contents
{:toc}

## Introduction

This page describes the basic logging function provided by Alluxio server processes (e.g., masters, workers and etc)
and application processes utilizing Alluxio clients (e.g., Spark or MapReduce jobs running on Alluxio).
Note that, there is an advanced feature that streams server-side and client-side logs to separate Alluxio logservers
(see [remote logging]({{ '/en/operation/Remote-Logging.html' | relativize_url }}) for more details).

## Log Location

Alluxio logging function is based on [`log4j`](https://logging.apache.org/log4j/) library,
configured through the `${ALLUXIO_HOME}/conf/log4j.properties`.
Logs are written to different files at different locations by Alluxio server processes and  application processes:

- Logs of Alluxio application processes are typically a part of application logs.
  The location varies by applications.
- Alluxio server processes (including master, worker, job master, job worker, or fuse integration)
  all write log files under `${ALLUXIO_HOME}/logs/`.

## Application-side Logs

We recommend looking at the documentation of the particular compute framework used to determine
where to find Alluxio client logs and how to set the logging level.

Below are links to the documentation of particular compute frameworks with information
on where logs may be found and how to configure them.

- [Apache Hadoop]({{ '/en/compute/Hadoop-MapReduce.html' | relativize_url }}#logging-configuration)
- [Apache HBase]({{ '/en/compute/HBase.html' | relativize_url }}#logging-configuration)
- [Apache Hive]({{ '/en/compute/Hive.html' | relativize_url }}#logging-configuration)
- [Apache Spark]({{ '/en/compute/Spark.html' | relativize_url }}#logging-configuration)

### Server-side Logs

### Configuring the Log Location

By default, Alluxio processes' log files can be found under `${ALLUXIO_HOME}/logs/`.
Each Alluxio server process (e.g., master, worker, FUSE, proxy) will log to a separate file with names like `master.log`, `worker.log`, `fuse.log` and `proxy.log`.

This location can be customized by the `alluxio.logs.dir` property.
Note that, this property MUST be set via JVM property; it cannot be set in the `alluxio-site.properties` file.
See the
[configuration settings page]({{ '/en/operation/Configuration.html' | relativize_url }}#configuration-sources)
for more information on how to set JVM properties for Alluxio.

## Configuring Log Levels

Alluxio supports the following four logging levels:
- `DEBUG`: fine-grained informational most useful for debugging purpose
- `INFO`: informational messages that highlight the status of  progress.
- `WARN`: potentially harmful events that user or system admins may need to know but the process will still continue running.
- `ERROR`: System error events that user or system admins should pay attention to.

By default, Alluxio server logs at level `INFO`, which will include all log records at level `INFO`, `WARN` and `ERROR`.


### Modifying Logging with `log4j.properties`

You can modify the `log4j.properties` found under the `${ALLUXIO_HOME}/conf/log4j.properties` to
modify logging levels.

For example, if you would like to modify the level for all logs, then you can change the
`rootLogger` level by modifying the following line:

```properties
log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}
```

To have `DEBUG` logging, modify the first line

```properties
log4j.rootLogger=DEBUG, ${alluxio.logger.type}, ${alluxio.remote.logger.type}`
```

### Modifying Server Logging at Runtime
It is recommended to modify the `log4j.properties` file, however if there is a need to modify
logging parameters without stopping nodes in the cluster, then you may modify some parameters at
runtime.

The Alluxio shell comes with a `logLevel` command that returns the current value of
or updates the log level of a particular class on specific instances.
Users are able to change Alluxio server-side log levels at runtime.

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
$ ./bin/alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=workers
```

For more information, refer to the help text of the `logLevel` command by running `./bin/alluxio logLevel`

## Enabling Debug-level Logging for APIs/RPCs

### Logging FUSE API calls

Setting in `conf/log4j.properties`:

```properties
log4j.logger.alluxio.fuse.AlluxioFuseFileSystem=DEBUG
```

You will see debug logs at the beginning and the end of each FUSE API call with its arguments and result
in `logs/fuse.log`:

```
2020-03-03 14:33:35,129 DEBUG AlluxioFuseFileSystem - Enter: chmod(path=/aaa,mode=100644)
2020-03-03 14:33:35,131 DEBUG AlluxioFuseFileSystem - Exit (0): chmod(path=/aaa,mode=100644) in 2 ms
2020-03-03 14:33:35,132 DEBUG AlluxioFuseFileSystem - Enter: getattr(path=/aaa)
2020-03-03 14:33:35,135 DEBUG AlluxioFuseFileSystem - Exit (0): getattr(path=/aaa) in 3 ms
2020-03-03 14:33:35,138 DEBUG AlluxioFuseFileSystem - Enter: getattr(path=/._aaa)
2020-03-03 14:33:35,140 DEBUG AlluxioFuseFileSystem - Failed to get info of /._aaa, path does not exist or is invalid
2020-03-03 14:33:35,140 DEBUG AlluxioFuseFileSystem - Exit (-2): getattr(path=/._aaa) in 2 ms
```

### Logging RPCs Calls Sent by Client

Add the following to your application-side `log4j.properties` to capture RPCs between the Alluxio client
and FileSystem Master:

```properties
log4j.logger.alluxio.client.file.FileSystemMasterClient=DEBUG
```

Similarly, capture lower-level RPCs between Alluxio client and Block Master:

```properties
log4j.logger.alluxio.client.block.BlockMasterClient=DEBUG
```

You will see debug logs at the beginning and end of each RPC with its arguments and result
in the client logs like the following:

```
2020-03-03 15:56:40,115 DEBUG FileSystemMasterClient - Enter: GetStatus(path=/.DS_Store,options=loadMetadataType: ONCE
commonOptions {
  syncIntervalMs: -1
  ttl: -1
  ttlAction: DELETE
}
)
2020-03-03 15:56:40,117 DEBUG FileSystemMasterClient - Exit (ERROR): GetStatus(path=/.DS_Store,options=loadMetadataType: ONCE
commonOptions {
  syncIntervalMs: -1
  ttl: -1
  ttlAction: DELETE
}
) in 2 ms: alluxio.exception.status.NotFoundException: Path "/.DS_Store" does not exist.
```

### Logging RPC Calls Received by Masters

On the master, debug-level RPC logging for File System level RPC calls can be turned on (e.g.,
creating/reading/writing/removing files, updating file attributions) using the `logLevel` command:

```console
$ ./bin/alluxio logLevel \
--logName=alluxio.master.file.FileSystemMasterClientServiceHandler \
--target master --level=DEBUG
```

Similarly, turn on the debug-level logging for block related RPC calls (e.g., adding/removing
blocks):

```console
$ ./bin/alluxio logLevel \
--logName=alluxio.master.block.BlockMasterClientServiceHandler \
--target master --level=DEBUG
```

### Identifying Expensive Client RPCs / FUSE calls

When debugging the performance, it is often useful to understand which RPCs take most of the time
but without recording all the communication (e.g., enabling all debug logging).
There are two properties introduced in v2.3.0 in Alluxio to record expensive calls or RPCs in logs with WARN level.
Setting in `conf/alluxio-site.properties` records client-side RPCs taking more than 200ms and FUSE APIs taking more than 1s:

```properties
alluxio.user.logging.threshold=200ms
alluxio.fuse.logging.threshold=1s
```

Example results are:

```
2020-03-08 23:40:44,374 WARN  BlockMasterClient - GetBlockMasterInfo(fields=[USED_BYTES,
FREE_BYTES, CAPACITY_BYTES]) returned BlockMasterInfo{capacityBytes=11453246122,
capacityBytesOnTiers={}, freeBytes=11453237930, liveWorkerNum=0, lostWorkerNum=0, usedBytes=8192, usedBytesOnTiers={}} in 600 ms
2020-03-08 23:40:44,374 WARN  AlluxioFuseFileSystem - statfs(path=/) returned 0 in 1200 ms
```
