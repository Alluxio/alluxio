---
layout: global
title: Logging Conventions
nickname: Logging Conventions
group: Resources
---

* Table of Contents
{:toc}

This page summarizes Alluxio's logging conventions and includes tips for modifying Alluxio's log4j
properties file to best suit deployment needs.

## Logging Conventions

Alluxio utilizes log levels in the following ways:

Error Level Logging

* Error level logs indicate system level problems which cannot be recovered from.
* Error level logs are always accompanied by a stack trace.

Warn Level Logging

* Warn level logs indicate a logical mismatch between user intended behavior and Alluxio behavior.
* Warn level logs are accompanied by an exception message.
* The associated stack trace may be found in debug level logs.

Info Level Logging

* Info level logs record important system state changes.
* Exception messages and stack traces are never associated with info level logs.

Debug Level Logging

* Debug level logs include detailed information for various aspects of the Alluxio system.
* Control flow logging (Alluxio system enter and exit calls) is done in debug level logs.

Trace Level Logging

* Trace level logs are not used in Alluxio.

## Configuration

Alluxio's logging behavior can be fully configured through the `log4j.properties` file found in the
`conf` folder.

By default, Alluxio logs to files in the `logs` directory which can be modified by setting the
`alluxio.logs.dir` system property. Each Alluxio process (Master, Worker, Clients, FUSE, Proxy)
logs to a different file.

### Remote Logging

By default, Alluxio processes log to local files. In certain environments, it is more reliable to
log to a central machine. Alluxio supports using Log4J's
[SocketAppender](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/SocketAppender.html)
to achieve this.

To set up remote logging, first determine the host which you plan to aggregate the logs at. On that
host, download the [Log4J jar](https://mvnrepository.com/artifact/log4j/log4j/1.2.17) which contains
a server implementation.

The server requires a `log4j.properties` file, use the file provided in Alluxio's `conf` folder. In
the file, replace all instances of `{alluxio.*}` variables with complete values, for example,
instead of `{alluxio.logs.dir}`, use `/opt/alluxio/logs`.

Then start the server with the following command

```bash
java -cp <PATH_TO_LOG4J_JAR> org.apache.log4j.net.SimpleSocketServer <PORT> <PATH_TO_LOG4J_PROPERTIES>
```

The server is now ready to log any incoming SocketAppender traffic. You can validate this by looking
in the logs directory, the first two lines in the log files should indicate the server was started
successfully.

Since this will be a long running process, it is worth considering using a separate service to
ensure the server is up. If the server is unavailable, Alluxio processes will not be affected but
logs will be lost.

On the Alluxio process side, update `conf/log4j.properties`, for example for `MASTER_LOGGER` replace
the existing properties with:

```
log4j.appender.MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER.Port=<PORT>
log4j.appender.MASTER_LOGGER.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER.ReconnectionDelay=<MILLIS_TO_WAIT_BEFORE_RECONNECTION_ATTEMPT>
log4j.appender.MASTER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

Then restart your Alluxio processes to pick up the latest logging configurations. Logs should now be
redirected to the remote server instead of being logged to a local file.

Note that logging to a remote server results in logs being aggregated instead of at a per machine
level.

It is often beneficial to log to both the local and remote machines. You can achieve this by
associating multiple appenders to your logger, taking the master log as an example:

```
log4j.rootLogger=INFO, ${alluxio.logger.type}_FILE, ${alluxio.logger.type}_SOCKET

log4j.appender.MASTER_LOGGER_FILE=org.apache.log4j.RollingFileAppender
log4j.appender.MASTER_LOGGER_FILE.File=${alluxio.logs.dir}/master_file.log
log4j.appender.MASTER_LOGGER_FILE.MaxFileSize=10MB
log4j.appender.MASTER_LOGGER_FILE.MaxBackupIndex=100
log4j.appender.MASTER_LOGGER_FILE.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER_FILE.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n

log4j.appender.MASTER_LOGGER_SOCKET=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER_SOCKET.Port=<PORT>
log4j.appender.MASTER_LOGGER_SOCKET.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER_SOCKET.ReconnectionDelay=<MILLIS_TO_WAIT_BEFORE_RECONNECTION_ATTEMPT>
log4j.appender.MASTER_LOGGER_SOCKET.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER_SOCKET.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

This is an example of using remote logging with Alluxio, users are encouraged to explore the various
appenders and configuration options provided by Log4J or 3rd parties to create a logging solution
best suited for their use case.
