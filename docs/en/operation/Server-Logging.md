---
layout: global
title: Server Logging
nickname: Server Logging
group: Operations
priority: 12
---

* Table of Contents
{:toc}

## Introduction

This page summarizes Alluxio's logging system for servers, such as the master and worker
processes. We include tips for modifying Alluxio's log4j properties file to best suit
deployment needs. If you are a user of Alluxio looking for logging information about clients
that are utilizing the Alluxio API, we recommend looking at the
[client logging documentation]({{ '/en/operation/Client-Logging.html' | relativize_url }})

Alluxio's logging behavior can be fully configured through the `log4j.properties` file found in the
`conf` folder.

## Log Location

### Default Location

By default Alluxio processes' log files can be found under `${ALLUXIO_HOME}/logs/`.

### Configuring the Log Location

The location of the logs is determined by the `alluxio.logs.dir` property. This can only be set via JVM
property; it cannot be set in the `alluxio-site.properties` file.
See the
[configuration settings page]({{ '/en/operation/Configuration.html' | relativize_url }}#configuration-sources)
for more information on how to set JVM properties for Alluxio.

Each Alluxio process (master, worker, FUSE, proxy) will log to a separate file within the
`alluxio.logs.dir` directory

## Log Levels

### Modifying Logging with `log4j.properties`

You can modify the `log4j.properties` found under the `${ALLUXIO_HOME}/conf/log4j.properties` to
modify logging levels.

For example, if you would like to modify the level for all logs, then you can change the
`rootLogger` level by modifying the following line:

```properties
log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}
```

If you wish to have `DEBUG` logging, then you would make the first line

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

Add to your application-side `log4j.properties` to capture RPCs between Alluxio client
and FileSystem Master:

```properties
log4j.logger.alluxio.client.file.FileSystemMasterClient=DEBUG
```

Similarly, capture lower-level RPCs between Alluxio client and Block Master:

```properties
log4j.logger.alluxio.client.block.BlockMasterClient=DEBUG
```

You will see debug logs on the begin and the end of each RPC with its arguments and result
in client logs like the following:

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

On master, one can turn on the debug-level RPC logging for File System level RPC calls (e.g.,
creating/reading/writing/removing files, updating file attributions) using `logLevel` command:

```console
$ ./bin/alluxio logLevel \
--logName=alluxio.master.file.FileSystemMasterClientServiceHandler \
--target master --level=DEBUG
```

Similarly, turn on the debug-level logging for Block related RPC calls (e.g., adding/removing
blocks):

```console
# For Block Master logging:
$ ./bin/alluxio logLevel \
--logName=alluxio.master.block.BlockMasterClientServiceHandler \
--target master --level=DEBUG
```

### Identifying Expensive Client RPCs / FUSE calls

When debugging the performance, it is often useful to understand which RPCs take most of the time
but without recording all the communication (e.g., enabling all debug logging). There are two
properties introduced in v2.3.0 in Alluxio to record expensive calls or RPCs in logs with WARN
level. For example, setting the following in `conf/alluxio-site.properties` will record client-side
RPCs taking more than 200ms and FUSE APIs taking more than 1s:

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

## Remote Logging

### Overview

Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.

### Deploying the Log Server

#### Configuring the Log Server

You need to specify the directory that the log server will write logs to by setting the
`ALLUXIO_LOGSERVER_LOGS_DIR` environment variable or adding it to
`${ALLUXIO_HOME}/conf/alluxio-env.sh`.

You can specify `ALLUXIO_LOGSERVER_PORT` to change the port the log server will be listening to.
You can find the default port in [table of configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.logserver.port)

#### Start the Log Server

On the log server, execute the following command.

```console
$ ./bin/alluxio-start.sh logserver
```

#### Stop the Log Server

```console
$ ./bin/alluxio-stop.sh logserver
```

### Configuring Alluxio Processes to use the Log Server

By default, remote logging is not enabled. You need to set 2 environment variables in `${ALLUXIO_HOME}/conf/alluxio-env.sh` to enable it.

1. `ALLUXIO_LOGSERVER_HOSTNAME` specifies the hostname of the remote log server.
The equivalent Java opt is `alluxio.logserver.hostname`.

1. `ALLUXIO_LOGSERVER_PORT` specifies the port the remote log server is listening to.
The equivalent Java opt is `alluxio.logserver.port`.

There is no requirement on where the log server can be run, as long as the other Alluxio servers
have network access to it. In our example, we run the log server on the same machine as a master.

#### Enable Remote Logging with Environment Variables

The two environment variables `ALLUXIO_LOGSERVER_HOSTNAME` and `ALLUXIO_LOGSERVER_PORT` control
the logging behavior of masters and workers in an Alluxio cluster.

Suppose the hostname of the log server is `AlluxioLogServer`, and the port is `45600`.
The following lines would need to be added to `conf/alluxio-env.sh` to enable remote logging :

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45600
```

These variables propagate their values to the `alluxio.logserver.hostname` and
`alluxio.logserver.port` [system properties] when set via `alluxio-env.sh` which are then referenced within `log4j.properties`

### Restart Alluxio and the Log Server

After making the modification to configuration, you need to restart the log server first. Then you
can start Alluxio. This ensures that the logs that Alluxio generates during start-up phase will
also go to the log server.

### Verify the Log Server has Started

SSH to the machine on which log server is running.

Go to the directory where the log server has been configured to store logs received from
other Alluxio servers. In the above example, the directory is `/tmp/alluxio_remote_logs`.

```console
$ cd /tmp/alluxio_remote_logs
$ ls
master          proxy           secondary_master    worker
$ ls -l master/
...
-rw-r--r--  1 alluxio  alluxio  26109 Sep 13 08:49 34.204.198.64.log
...
```

You can see that the log files are put into different folders according to their type. Master logs are put
in the folder `master`, worker logs are put in folder `worker`, etc. Within each folder, log files from
different workers are distinguished by the IP/hostname of the machine on which the server has been running.

### Control Remote Logging Behavior with `log4j.properties`

The remote log server uses the default threshold of `WARN`, which means log4j levels below `WARN` will not be sent to the remote log server.
This can be finer tuned by modifying `${ALLUXIO_HOME}/conf/log4j.properties`.

For example, if you want to change log level for the remote logger for Alluxio master logs,
you can modify the corresponding appender's threshold.

```properties
# Remote appender for Master
log4j.appender.REMOTE_MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.REMOTE_MASTER_LOGGER.Port=${alluxio.logserver.port}
log4j.appender.REMOTE_MASTER_LOGGER.RemoteHost=${alluxio.logserver.hostname}
log4j.appender.REMOTE_MASTER_LOGGER.ReconnectionDelay=10000
log4j.appender.REMOTE_MASTER_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.REMOTE_MASTER_LOGGER.filter.ID.ProcessType=MASTER
# Set the threshold to your desired log level
log4j.appender.REMOTE_MASTER_LOGGER.Threshold=WARN
```

This enables you to further customize the appender in `log4j.properties` to, for example, specify the log format.
How to do that is beyond the scope of this documentation.

## Configuration Properties

You can find the properties related to logging in the
[table of configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.logger.type)
