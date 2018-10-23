---
layout: global
title: Client Logging
nickname: Client Logging
group: Advanced
priority: 1
---

This page summarizes how to find client-specific logs and modify logging levels in order to debug
client-side issues with Alluxio.

* Table of Contents
{:toc}

## Log Location and Log Levels

We recommend looking at the documentation of the particular compute framework used to determine
where to find Alluxio client logs and how to set the logging level.

Below are links to the documentation of particular compute frameworks with information 
on where logs may be found and how to configure them.

- [Apache Hadoop]({{ site.baseurl }}{% link en/compute/Hadoop-MapReduce.md %}#logging-configuration)
- [Apache HBase]({{ site.baseurl }}{% link en/compute/HBase.md %}#logging-configuration)
- [Apache Hive]({{ site.baseurl }}{% link en/compute/Hive.md %}#logging-configuration)
- [Apache Spark]({{ site.baseurl }}{% link en/compute/Spark.md %}#logging-configuration)

Note that while Alluxio uses log4j, the location of client logs varies by compute framework
but will typically be found in the same location as the application logs.

## Remote Logging

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
log4j.appender.CLIENT_REMOTE_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.CLIENT_REMOTE_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

Enable the `CLIENT_REMOTE_LOGGER` appender by adding it to an existing logger within the a log4j
configuration:

```properties
log4j.rootLogger=DEBUG, CLIENT_REMOTE_LOGGER
```

Again, refer to the documentation for the corresponding framework linked in the previous section
to locate the log4j configuration file.