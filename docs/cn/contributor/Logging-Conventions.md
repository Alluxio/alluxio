---
layout: global
title: Logging Configuration
nickname: 运行日志选项
group: Contributor Resources
priority: 3
---

* 内容列表
{:toc}

本页总结了Alluxio的日志记录约定，并包括了对Alluxio的log4j属性文件进行修改以最适合部署需要的相关提示。

Alluxio的日志行为可以完全由`conf`文件夹下的`log4j.properties`文件进行配置。

## Alluxio日志

默认情况，Alluxio将日志保存到`logs`目录下，可以通过修改`alluxio.logs.dir`系统属性来配置日志所在目录。每个Alluxio进程（Master, Worker, Clients, FUSE, Proxy）的日志对应不同的文件。

## 远程日志记录

默认情况，Alluxio将日志文件记录在本地，在特定环境下，将日志记录到中心机器更加可靠，Alluxio支持使用Log4J的
[SocketAppender](https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/net/SocketAppender.html)
来实现这一点。

要设置远程日志记录，首先要确定汇集日志的主机，然后在该主机上，下载[Log4J jar](https://mvnrepository.com/artifact/log4j/log4j/1.2.17)（包含了日志记录服务器端的实现）。

服务器端需要一个`log4j.properties`文件，Alluxio的`conf`文件夹中提供了该文件，请用完整值替换文件中`{alluxio.*}`变量的所有实例，例如，
使用`/opt/alluxio/logs`替换`{alluxio.logs.dir}`。

然后使用以下命令启动服务器:

```bash
java -cp <PATH_TO_LOG4J_JAR> org.apache.log4j.net.SimpleSocketServer <PORT> <PATH_TO_LOG4J_PROPERTIES>
```

服务器端现在可以记录任何到来的SocketAppender流量，可以通过查看日志目录中的日志文件来验证这一点，在日志文件中的前两行记录着服务器已成功启动的信息。

由于这将是一个长期运行的过程，因此有必要考虑使用单独的服务来确保服务器已启动，如果服务器不可用，Alluxio进程不会受到影响，但是日志记录将会丢失。

在Alluxio进程端，更新 `conf/log4j.properties`，例如，对于`MASTER_LOGGER`，用如下属性替代已有的属性：

```
log4j.appender.MASTER_LOGGER=org.apache.log4j.net.SocketAppender
log4j.appender.MASTER_LOGGER.Port=<PORT>
log4j.appender.MASTER_LOGGER.RemoteHost=<HOSTNAME_OF_LOG_SERVER>
log4j.appender.MASTER_LOGGER.ReconnectionDelay=<MILLIS_TO_WAIT_BEFORE_RECONNECTION_ATTEMPT>
log4j.appender.MASTER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.MASTER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```

然后重新启动Alluxio进程以获取最新的日志记录配置。日志现在应该是重定向到远程服务器，而不是记录到本地文件。

请注意，和每台机器分别记录的级别所不同的是，记录到远程服务器端会导致日志的聚合。

通常，将日志同时保存到本地和远程计算机是很有益处的。可以通过将多个appender关联到logger，以master日志为例：

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

这是Alluxio使用远程日志的一个例子。Alluxio鼓励用户探索各种由Log4J或第三方提供的appender和配置选项，从而创建最适合实际用例的日志记录解决方案。

## 在Alluxio服务器运行时动态更改日志级别

Alluxio shell附带了一个`logLevel`命令，可以在特定实例上获取或更改特定类的日志级别。

语法是`alluxio logLevel --logName = NAME [--target = <master | worker | host：port>] [--level = LEVEL]`，其中`logName`表示日志的名称，`target`列出了需要设定的Alluxio master或worker列表 。 如果提供了参数`level`，则命令更改日志级别，否则将获取并显示当前日志级别。

例如，以下命令将`alluxio.heartbeat.HeartbeatContext`类的日志级别在master和`192.168.100.100：30000`的worker上设置为调试级别。

```bash
alluxio logLevel --loggerName = alluxio.heartbeat.HeartbeatContext --target = master，192.168.100.100：30000 --level = DEBUG
```

以下命令获取`alluxio.heartbeat.HeartbeatContext`类的所有worker的日志级别
```bash
alluxio logLevel --loggerName = alluxio.heartbeat.HeartbeatContext --target = workers
```

## 客户端日志记录配置

改变在计算框架（例如Spark，Presto）进程中运行的Alluxio客户端的日志级别并且为了调试把它保存为一个文件通常是有用的。要做到这一点，你可以将下面的java选项传递给计算框架进程。

例如，选项`-Dalluxio.log.dir=/var/alluxio/ -Dalluxio.logger.type=USER_LOGGER -Dlog4j.configuration=/tmp/
alluxio/conf/log4j.properties`将指导Alluxio客户端在Alluxio的conf路径上使用log4j配置并且输出日志到路径为`/var/alluxio/`，名为`user_USER_NAME.log`的文件中，`USER_NAME`是开始该客户端程序的用户。如果客户端机器和安装Alluxio的机器不一样，你可以将`conf/log4j.properties`中的文件复制到客户端机器并且把它的路径传给`log4j.configuration`选项。如果你不想覆盖应用的`log4j.properties`路径，你可以将下面的内容添加到它的`log4j.properties`文件中：

```
# Appender for Alluxio User
log4j.rootLogger=INFO,${alluxio.logger.type}
log4j.appender.USER_LOGGER=org.apache.log4j.RollingFileAppender
log4j.appender.USER_LOGGER.File=${alluxio.logs.dir}/user_${user.name}.log
log4j.appender.USER_LOGGER.MaxFileSize=10MB
log4j.appender.USER_LOGGER.MaxBackupIndex=10
log4j.appender.USER_LOGGER.layout=org.apache.log4j.PatternLayout
log4j.appender.USER_LOGGER.layout.ConversionPattern=%d{ISO8601} %-5p %c{1} - %m%n
```
