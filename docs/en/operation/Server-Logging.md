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
[client logging documentation]({{ '/en/advanced/Client-Logging.html' | relativize_url }})

Alluxio's logging behavior can be fully configured through the `log4j.properties` file found in the
`conf` folder.

## Log Location

### Default Location

By default Alluxio processes' log files can be found under `${ALLUXIO_HOME}/logs/`.

### Configuring the Log Location

The location of the logs is determined by the `alluxio.logs.dir` property. This can only be set via JVM
property; it cannot be set in the `alluxio-site.properties` file.
See the
[configuration settings page]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}#configuration-sources)
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

### Modifying Logging at Runtime
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
The `host:webPort` form only works with worker hostnames and ports.
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

For more information, refer to the help text of the `logLevel` command by running `./bin/alluxio logLevel`

## Remote Logging

### Overview

Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.

### Deploying the Log Server

#### Configuring the Log Server

You can choose the directory that the log server will write logs to by setting the
`ALLUXIO_LOGSERVER_LOGS_DIR` environment variable or adding it to
`${ALLUXIO_HOME}/conf/alluxio-env.sh`

#### Start the Log Server

On the log server, execute the following command.

```bash
$ ./bin/alluxio-start.sh logserver
```

#### Stop the Log Server

```bash
$ ./bin/alluxio-stop.sh logserver
```

### Configuring Alluxio Processes to use the Log Server

By default, remote logging is not enabled. There are two options which can enable remote logging.
One option is to set environment variables within `${ALLUXIO_HOME}/conf/alluxio-env.sh`. The other
is to modify Alluxio's `log4j.properties` file under `${ALLUXIO_HOME}/conf/log4j.properties`.

There is no requirement on where the log server can be run, as long as the other Alluxio servers
have network access to it. In our example, we run the log server on the same machine as a master.

#### Enable Remote Logging with Environment Variables

The two environment variables `ALLUXIO_LOGSERVER_HOSTNAME` and `ALLUXIO_LOGSERVER_PORT` control
the logging behavior of masters and workers in an Alluxio cluster.

Suppose the hostname of the log server is `AlluxioLogServer`, and the port is `45010`.
The following lines would need to be added to `conf/alluxio-env.sh` to enable the correct :

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
```

These variables propagate their values to the `alluxio.logserver.hostname` and
`alluxio.logserver.port`
[system properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.logserver.hostname)
when set via `alluxio-env.sh` which are then referenced within `log4j.properties`

#### Enable Remote Logging with `log4j.properties`

You can also choose to modify the `${ALLUXIO_HOME}/conf/log4j.properties` file on the machines where
your masters or workers reside to add an appender which will send log entries to the log server. A
sample configuration is provided below:

```properties
log4j.rootLogger=INFO, MASTER_LOGGER_SOCKET
log4j.appender.MASTER_LOGGER_SOCKET=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER_SOCKET.Port=<PORT_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER_SOCKET.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER_SOCKET.ReconnectionDelay=10000
log4j.appender.MASTER_LOGGER_SOCKET.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER_SOCKET.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

Note that on the line containing `log4j.rootLogger` you may add multiple appenders in order to
log locally to a file on the system and remotely over a network. For example:

```properties
log4j.rootLogger=INFO, ${REMOTE_APPENDER_NAME}, ${LOCAL_APPENDER_NAME}
```

### Restart Alluxio and the Log Server

After making the modification to configuration, you need to restart the log server first. Then you
can start Alluxio. This ensures that the logs that Alluxio generates during start-up phase will
also go to the log server.

### Verify the Log Server has Started

SSH to the machine on which log server is running.

Go to the directory where the log server has been configured to store logs received from
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

## Configuration Properties

You can find the properties related to logging in the
[table of configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.logger.type)
