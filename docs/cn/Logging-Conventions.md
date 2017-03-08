---
layout: global
title: Logging Conventions
nickname: Logging Conventions
group: Resources
---

* 内容列表
{:toc}

本页总结了Alluxio的日志记录约定，并包括了对Alluxio的log4j属性文件进行修改以最适合部署需要的相关提示。

## 日志记录约定

Alluxio使用如下的日志级别：

错误级别日志

* 错误级别日志表示无法恢复的系统级问题。
* 错误级别日志总是伴随着堆栈跟踪信息。

警告级别日志

* 警告级别日志表示用户预期行为和Alluxio实际行为之间的逻辑不匹配。
* 警告级别日志伴有异常消息。
* 相关的堆栈跟踪信息可以在调试级日志中找到。

信息级别日志

* 信息级别日志记录了重要系统状态的更改信息。
* 异常消息和堆栈跟踪与信息级别日志从无关联。

调试级别日志

* 调试级别日志包括Alluxio系统各方面的详细信息。
* 控制流日志记录（Alluxio系统的进入和退出调用）在调试级别日志中完成。

跟踪级别日志

* Alluxio中不使用跟踪级别日志。

## 配置

Alluxio的日志行为可以完全由`conf`文件夹下的`log4j.properties`文件进行配置。

默认情况，Alluxio将日志保存到`logs`目录下，可以通过修改`alluxio.logs.dir`系统属性来配置日志所在目录。每个Alluxio进程（Master, Worker, Clients, FUSE, Proxy）的日志对应不同的文件。

### 远程日志记录

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
