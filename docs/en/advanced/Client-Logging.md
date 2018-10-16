---
layout: global
title: Client Logging
nickname: Client Logging
group: Advanced
priority: 1
---

This page summarizes how to find client-specific logs and modify logging levels in order to debug
client-side issues with your Alluxio installation.

* Table of Contents
{:toc}

## Log Location

Depending on which compute framework you are using, the location of the logs can vary. We
recommend looking at the specific documentation for your compute framework to determine where to
find alluxio client logs.

Below are links to compute framework documentation with information about where logs may be found
and how to configure them.

- [Apache Hadoop]({{ site.baseurl }}{% link en/compute/Hadoop-MapReduce.md %}#logging-configuration)
- [Apache HBase]({{ site.baseurl }}{% link en/compute/HBase.md %}#logging-configuration)
- [Apache Hive]({{ site.baseurl }}{% link en/compute/Hive.md %}#logging-configuration)
- [Apache Spark]({{ site.baseurl }}{% link en/compute/Spark.md %}#logging-configuration)

It is important to note that while Alluxio specifically uses log4j to perform logging, the location
of client logs varies by application and will typically be found in the same location as the
application logs for each compute framework.

## Log Levels

Often it's useful to change the log level of the Alluxio client running in the compute framework
(e.g. Spark, Hadoop) process, and save it to a file for debugging. To achieve this, you need to
look at your compute framework's logging options which are linked in the above section and modify
the appropriate configuration property.

## Remote Logging

Logging can be configured to send logs to a remote server via a
[`SocketAppender`](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/SocketAppender.html)

A `SocketAppender` appender can be included in your logging configuration by adding to the
`log4j.properties` file that your compute framework utilizes for logging.

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

Using this configuration, the `CLIENT_REMOTE_LOGGER` appender should be added to an already
existing logger within the a log4j configuration. Foe example

```properties
log4j.rootLogger=DEBUG, CLIENT_REMOTE_LOGGER
```

To find where the proper configuration file to modify is, refer to the documentation for your
desired framework linked in the previous section.