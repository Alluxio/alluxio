---
layout: global
title: Client Logging
nickname: Client Logging
group: Advanced
priority: 1
---

* Table of Contents
{:toc}

## Introduction

This page summarizes how to find client-specific logs and modify logging levels in order to debug
client-sided issues with your Alluxio installation.

## Log Location

Depending on which compute framework you are using, the location of the logs can vary. We
recommend looking at the specific documentation for your compute framework to determine where to
find alluxio cient logs.

Here are links to compute frameworks with information where logs may be found and how to configure

- [Apache Spark]({{ site.baseurl }}{% link en/compute/Compute-Spark.md %})
- [Apache Hadoop]({{ site.baseurl }}{% link en/compute/Compute-Hadoop-MapReduce.md %})
- [Apache HBase]({{ site.baseurl }}{% link en/compute/Compute-HBase.md %})
- [Apache Hive]({{ site.baseurl }}{% link en/compute/Compute-Hive.md %})
- [Presto]({{ site.baseurl }}{% link en/compute/Compute-Presto.md %})

However, by setting the `alluxio.logs.dir` property you can modify where the logs are stored.
This Alluxio property is a JVM system property which cannot be set from an
`alluxio-site.properties` file. For more information on setting JVM system properties see
the [configuration documentation]({{site.baseurl}}{% link en/advanced/Configuration-Settings.md %}#configuration-sources)

## Log Levels

Often it's useful to change the logLevel of the Alluxio client running in the compute framework
(e.g. Spark, Presto) process, and save it to a file for debugging. To achieve this, you can pass
the following Java options to the compute framework process.

For example, the options `-Dalluxio.logs.dir=/var/alluxio/ -Dalluxio.logger.type=USER_LOGGER
-Dlog4j.configuration=/tmp/alluxio/conf/log4j.properties` will instruct Alluxio client to use the
log4j configuration in the Alluxio's `conf` path and output the log to a file `user_USER_NAME.log`
at the path `/var/alluxio/`, where `USER_NAME` is the user that starts the client program.

If the client is not on the same machine where Alluxio is installed, you can make a copy of the
file in `conf/log4j.properties` to the client machine, and pass its path to the option
`log4j.configuration`. If you do not want to override the application's `log4j.properties` path,
alternatively you can append the followings to its `log4j.properties` file:

```conf
# Appender for Alluxio User
log4j.rootLogger=INFO, ${alluxio.logger.type}
log4j.appender.USER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.USER_LOGGER.File=${alluxio.logs.dir}/user_${user.name}.log
log4j.appender.USER_LOGGER.MaxFileSize=10MB
log4j.appender.USER_LOGGER.MaxBackupIndex=10
log4j.appender.USER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.USER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

## Remote Logging

Logging can be configured to send logs to a remote server via a
[`SocketAppender`](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/SocketAppender.html)

A `SocketAppender` appender can be included in your logging configuration by adding to the
`log4j.properties` file that your compute framework utilizes for logging.

An example configuration for a `SocketAppender` can be found below:

```conf
# Appender to send logs to a log server
log4j.appender.MASTER_LOGGER_SOCKET=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER_SOCKET.Port=<PORT>
log4j.appender.MASTER_LOGGER_SOCKET.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER_SOCKET.ReconnectionDelay=<MILLIS_TO_WAIT_BEFORE_RECONNECTION_ATTEMPT>
log4j.appender.MASTER_LOGGER_SOCKET.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER_SOCKET.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```