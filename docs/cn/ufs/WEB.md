---
layout: global
title: Alluxio集成WEB作为底层存储
nickname: Alluxio集成WEB作为底层存储
group: Storage Integrations
priority: 6
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用WEB作为底层文件系统。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }})，或者[下载二进制包]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }})

## 配置Alluxio

Alluxio通过[统一命名空间]({{ '/cn/core-services/Unified-Namespace.html' | relativize_url }})统一访问不同存储系统。 WEB的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装
您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用WEB作为其底层存储系统。如果该配置文件不存在，请从模板创建该配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

将以下的环境变量要添加到`conf/alluxio-site.properties`配置文件中，实际上，我们可以支持http://及https://协议.

```
alluxio.master.hostname=localhost
# alluxio.master.mount.table.root.ufs=[https|http]://<HOSTNAME>:<PORT>/DIRECTORY/
# 本文中的样例设置
alluxio.master.mount.table.root.ufs=https://downloads.alluxio.io/downloads/files/
```

本文例子设置为

指定WEB页面解析相关配置(可选):
```
alluxio.underfs.web.connnection.timeout=<WEB_CONNECTION_TIMEOUT>
alluxio.underfs.web.header.last.modified=<WEB_HEADER_LAST_MODIFIED>
alluxio.underfs.web.parent.names=<WEB_PARENT_NAMES>
alluxio.underfs.web.titles=<WEB_TITLES>
```
这里的alluxio.underfs.web.connnection.timeout是HTTP请求链接超时设置(单位：秒), 默认60s。alluxio.underfs.web.header.last.modified是解析HTTP请求中返回的HEADER中的最后修改时间字段的格式, 默认为"EEE, dd MMM yyyy HH:mm:ss zzz"。 alluxio.underfs.web.parent.names是判断文件列表开始行的索引值的标识(可以多个，逗号分隔), 默认为"Parent Directory,..,../"。 alluxio.underfs.web.titles是我们判断一个页面为目录的标识(可以多个，逗号分隔), 默认为"Index of ,Directory listing for "。

### 嵌套目录安装

WEB可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。 
[Mount 命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将WEB容器内部的目录挂载到Alluxio的`/web`目录

```console
$ ./bin/alluxio fs mount --option alluxio.underfs.web.connnection.timeout=<WEB_CONNECTION_TIMEOUT> \
  --option alluxio.underfs.web.header.last.modified=<WEB_HEADER_LAST_MODIFIED> \
  --option alluxio.underfs.web.parent.names=<WEB_PARENT_NAMES> \
  --option alluxio.underfs.web.titles=<WEB_TITLES> \
  /web [https|http]://<HOSTNAME>:<PORT>/DIRECTORY/ 
```

## 使用WEB运行Alluxio

简单地运行以下命令来启动Alluxio文件系统：

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

要验证Alluxio是否正在运行，你可以访问**[http://localhost:19999](http://localhost:19999)**，或者查看`logs`下的日志。

运行成功后，访问你的WEB volume查看目录列表，执行以下命令：

```console
$ ./bin/alluxio fs ls /
```

等待片刻, 你可以看到如下结果

```
dr--r-----                                              0       PERSISTED 05-21-2019 12:53:22:000  DIR /1.4.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:54:23:000  DIR /1.5.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:55:06:000  DIR /1.6.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:55:38:000  DIR /1.6.1
dr--r-----                                              0       PERSISTED 05-21-2019 12:57:00:000  DIR /1.7.0
dr--r-----                                              0       PERSISTED 05-21-2019 12:57:57:000  DIR /1.7.1
dr--r-----                                              0       PERSISTED 05-21-2019 13:00:25:000  DIR /1.8.0
dr--r-----                                              0       PERSISTED 05-21-2019 13:02:07:000  DIR /1.8.1
dr--r-----                                              0       PERSISTED 05-24-2019 05:16:31:000  DIR /2.0.0
dr--r-----                                              0       PERSISTED 05-21-2019 13:02:11:000  DIR /2.0.0-preview
```

你可以在任何时间运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```
