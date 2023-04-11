---
layout: global
title: 远程记录日志
group: Operations
priority: 6
---

* 内容列表
{:toc}

本页总结了Alluxio的日志记录约定，并包括了对Alluxio的log4j属性文件进行修改以最适合部署需要的相关提示。

Alluxio的日志行为可以完全由`conf`文件夹下的`log4j.properties`文件进行配置。

## Alluxio日志

默认情况，Alluxio将日志保存到`logs`目录下，可以通过修改`alluxio.logs.dir`系统属性来配置日志所在目录。每个Alluxio进程（Master, Worker, Clients, FUSE, Proxy）的日志对应不同的文件。

## 在Alluxio服务器运行时动态更改日志级别

Alluxio shell附带了一个`logLevel`命令，可以在特定实例上获取或更改特定类的日志级别。

语法是`alluxio logLevel --logName = NAME [--target = <master | worker | host：port>] [--level = LEVEL]`，其中`logName`表示日志的名称，`target`列出了需要设定的Alluxio master或worker列表 。 如果提供了参数`level`，则命令更改日志级别，否则将获取并显示当前日志级别。

例如，以下命令将`alluxio.heartbeat.HeartbeatContext`类的日志级别在master和`192.168.100.100：30000`的worker上设置为调试级别。

```console
$ ./bin/alluxio logLevel --loggerName=alluxio.heartbeat.HeartbeatContext \
  --target=master,192.168.100.100:30000 --level=DEBUG
```

以下命令获取`alluxio.heartbeat.HeartbeatContext`类的所有worker的日志级别
```console
$ ./bin/alluxio logLevel --loggerName=alluxio.heartbeat.HeartbeatContext --target=workers
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

