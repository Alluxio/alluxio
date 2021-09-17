---
layout: global
title: Remote Logging
nickname: Remote Logging
group: Operations
priority: 12
---

* Table of Contents
{:toc}


## Overview

Alluxio supports sending logs to a remote log server over the network. This feature can be useful
to system administrators who have to perform the task of log collection. With remote logging, the
log files, e.g. master.log, worker.log, etc. on all Alluxio servers will be readily available on
a designated and configurable directory on the log server.
Alternatively, users can use [basic logging]({{ '/en/operation/Basic-Logging.html' | relativize_url }})
functions provided by Alluxio rather than using remote logging.

## Deploying the Log Server

### Configuring the Log Server

You need to specify the directory that the log server will write logs to by setting the
`ALLUXIO_LOGSERVER_LOGS_DIR` environment variable or adding it to
`${ALLUXIO_HOME}/conf/alluxio-env.sh`. By default the log server will write the logs to `${ALLUXIO_HOME}/logs`.

You can specify `ALLUXIO_LOGSERVER_PORT` to change the port the log server will be listening to.
You can find the default port in [table of configuration properties]({{ '/en/reference/Properties-List.html' | relativize_url }}#alluxio.logserver.port)

### Start the Log Server

On the log server, execute the following command.

```console
$ ./bin/alluxio-start.sh logserver
```

### Stop the Log Server

```console
$ ./bin/alluxio-stop.sh logserver
```

## Configuring Alluxio Server Processes

By default, remote logging is not enabled. You need to set 2 environment variables in `${ALLUXIO_HOME}/conf/alluxio-env.sh` to enable it.

1. `ALLUXIO_LOGSERVER_HOSTNAME` specifies the hostname of the remote log server.
The equivalent Java opt is `alluxio.logserver.hostname`.

1. `ALLUXIO_LOGSERVER_PORT` specifies the port the remote log server is listening to.
The equivalent Java opt is `alluxio.logserver.port`.

There is no requirement on where the log server can be run, as long as the other Alluxio servers
have network access to it. In our example, we run the log server on the same machine as a master.

### Enabling Remote Logging with Environment Variables

The two environment variables `ALLUXIO_LOGSERVER_HOSTNAME` and `ALLUXIO_LOGSERVER_PORT` control
the logging behavior of masters and workers in an Alluxio cluster.

Suppose the hostname of the log server is `AlluxioLogServer`, and the port is `45600`.
The following lines would need to be added to `conf/alluxio-env.sh` to enable remote logging :

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT="45600"
```
> Note: You MUST set BOTH variables.

These variables propagate their values to the `alluxio.logserver.hostname` and
`alluxio.logserver.port` [system properties] when set via `alluxio-env.sh` which are then referenced within `log4j.properties`

## Start the Log Server and Alluxio

After making the modification to configuration, you need to restart the log server first. Then you
can start Alluxio. This ensures that the logs that Alluxio generates during start-up phase will
also go to the log server.

## Verify the Log Server has Started

SSH to the machine on which log server is running.

Go to the directory where the log server has been configured to store logs received from
other Alluxio servers. The default logs directory is `${ALLUXIO_HOME}/logs`. 
This is configured by the environment variable `ALLUXIO_LOGSERVER_LOGS_DIR`.

```console
$ cd ${ALLUXIO_HOME}/logs
$ ls
master    job_master    worker    job_worker
$ ls -l master/
...
-rw-r--r--  1 alluxio  alluxio  26109 Sep 13 08:49 34.204.198.64.log
...
```

You can see that the log files are put into different folders according to their type. Master logs are put
in the folder `master`, worker logs are put in folder `worker`, etc. Within each folder, log files from
different workers are distinguished by the IP/hostname of the machine on which the server has been running.

## Control Remote Logging Behavior with `log4j.properties`

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

## Configuring Applications Process

Logging can be configured to send log files to a remote server via a
[`SocketAppender`](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/SocketAppender.html)

Include a `SocketAppender` in the logging configuration by adding it to the
`log4j.properties` file that the compute framework utilizes for logging.

An example configuration for a `SocketAppender` can be found below:

```properties
# Appender to send logs to a log server
log4j.appender.CLIENT_REMOTE_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.CLIENT_REMOTE_LOGGER.Port=<PORT_OF_LOG_SERVER>
log4j.appender.CLIENT_REMOTE_LOGGER.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.CLIENT_REMOTE_LOGGER.ReconnectionDelay=10000
log4j.appender.CLIENT_REMOTE_LOGGER.filter.ID=alluxio.AlluxioRemoteLogFilter
log4j.appender.CLIENT_REMOTE_LOGGER.filter.ID.ProcessType=CLIENT
log4j.appender.CLIENT_REMOTE_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.CLIENT_REMOTE_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

Enable the `CLIENT_REMOTE_LOGGER` appender by adding it to an existing logger within the a log4j
configuration:

```properties
log4j.rootLogger=DEBUG, CLIENT_REMOTE_LOGGER
```

> Note: refer to the documentation for the corresponding framework to locate the log4j configuration file.

## Advanced Setup

### Remote Logging in K8s

Enabling remote logging in K8s is different from that in a physical cluster.

See [Enable Remote Logging in K8s]({{ '/en/deploy/Running-Alluxio-On-Kubernetes.html#enable-remote-logging' | relativize_url }})
for detailed instructions.
