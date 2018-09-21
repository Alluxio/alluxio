---
layout: global
title: Server Logging
nickname: Server Logging
group: Operations
priority: 0
---

* Table of Contents
{:toc}

## Introduction

This page summarizes Alluxio's logging system for servers, such as the master and worker
processes. We include tips for modifying Alluxio's log4j properties file to best suit
deployment needs. If you are a user of Alluxio looking for logging information about clients
that are utilizing the Alluxio API, we recommend looking at the
[client logging documentation]({{site.baseurl}}{% link en/advanced/Client-Logging.md %})

Alluxio's logging behavior can be fully configured through the `log4j.properties` file found in the
`conf` folder.

## Log Location

### Default Location

By default Alluxio processes log files can be found under `{ALLUXIO_HOME}/logs/`.

### Configuring the Log Location 

The location of the logs on is determined by the `alluxio.logs.dir` property. This is a JVM
property which cannot be set in the `alluxio-site.properties` file. See the
[configuration settings page]({{site.baseurl}}{% link en/advanced/Configuration-Settings.md %}#configuration-sources)
for more information on how to set JVM properties for Alluxio.

Each Alluxio process (master, worker, FUSE, proxy) will log to a separate file within the
`alluxio.logs.dir` directory

## Log Levels

### Modifying Logging with `log4j.properties`

You can modify the `log4j.properties` found under the `{ALLUXIO_HOME}/conf/log4j.properties` to modify logging levels.

### Modifying Logging at Runtime

Alluxio shell comes with a `logLevel` command that allows you to get or change the log level of a
particular class on specific instances.

The synax is `alluxio logLevel --logName=NAME [--target=<master|worker|host:port>] [--level=LEVEL]`,
where the `logName` indicates the logger's name, and `target` lists the Alluxio masters or
workers to set. If parameter `level` is provided the command changes the logger level, otherwise it
gets and displays the current logger level.

For example, this command sets the class `alluxio.heartbeat.HeartbeatContext`'s logger level to
DEBUG on master as well as a worker at `192.168.100.100:30000`.

```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=master,192.168.100.100:30000 --level=DEBUG
```

And the following command gets all workers' log level on class `alluxio.heartbeat.HeartbeatContext`
```bash
alluxio logLevel --logName=alluxio.heartbeat.HeartbeatContext --target=workers
```

## Remote Logging

### Overview

Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.

### Deploying The Log Server

#### Start Log Server

On the log server, execute the following command.

```bash
$ ./bin/alluxio-start.sh logserver
```

#### Stop the Log Server

```bash
$ ./bin/alluxio-stop.sh logserver
```

### Configuring Alluxio Processes to use the Log Server

Refer to [Running Alluxio on a cluster]({{site.baseurl}}{% link en/deploy/Running-Alluxio-On-a-Cluster.md %})
for instructions in deploying Alluxio in a cluster.

By default, remote logging is not enabled. To enable Alluxio remote logging, you can set
environment variables or modify Alluxio's `log4j.properties`

There is no requirement on where the log server can be run run, as long as the other Alluxio servers
have network access to it. In our example, we run the log server on the same machine as a master.

#### Enable Remote Logging with Environment Variables

The three environment variables `ALLUXIO_LOGSERVER_HOSTNAME`, `ALLUXIO_LOGSERVER_PORT` and
`ALLUXIO_LOGSERVER_LOGS_DIR` control Alluxio remote logging behavior.

Suppose the hostname of the log server is `AlluxioLogServer`, and the port is `45010`.
In `conf/alluxio-env.sh`, add the following lines:

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

#### Enable Remote Logging with `log4j.properties`

You can also choose to modify the `{ALLUXIO_HOME}/conf/log4j.properties` file on the machines where
your masters or workers reside to add an appender which will send log entries to the log server. A
sample configuration is provided below:

```conf
log4j.appender.MASTER_LOGGER_SOCKET=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER_SOCKET.Port=<PORT>
log4j.appender.MASTER_LOGGER_SOCKET.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER_SOCKET.ReconnectionDelay=<MILLIS_TO_WAIT_BEFORE_RECONNECTION_ATTEMPT>
log4j.appender.MASTER_LOGGER_SOCKET.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER_SOCKET.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```


### Restart Alluxio And Log Server

After making the modification to configuration, you need to restart the log server first. Then you
can start Alluxio. This ensures that the logs that Alluxio generates during start-up phase will
also go to the log server.


### Verify Log Server Has Started

First, `ssh` to the machine on which log server is running.

Second, go to the directory where the log server has been configured to store logs received from
other Alluxio servers. In the above example, the directory is `/tmp/alluxio_remote_logs`.

```bash
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