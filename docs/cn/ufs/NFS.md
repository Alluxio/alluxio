---
layout: global
title: Alluxio集成NFS作为底层存储
nickname: Alluxio集成NFS作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[NFS](http://nfs.sourceforge.net)作为底层文件系统。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Alluxio

您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用NFS作为其底层存储系统。如果该配置文件不存在，请从模板创建该配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

假定所有NFS客户端与Alluxio部署在同样的节点上，且NFS挂载在`/mnt/nfs`，那以下的环境变量要添加到`conf/alluxio-site.properties`配置文件中：

```
alluxio.master.hostname=localhost
alluxio.master.mount.table.root.ufs=/mnt/nfs
```

## 使用NFS运行Alluxio

简单地运行以下命令来启动Alluxio文件系统：

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

要验证Alluxio是否正在运行，你可以访问**[http://localhost:19999](http://localhost:19999)**，或者查看`logs`下的日志。

接着，你可以运行一个简单的示例程序：

```console
$ ./bin/alluxio runTests
```

运行成功后，访问你的NFS volume，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

```
/mnt/nfs/default_tests_files/Basic_CACHE_THROUGH
```

你可以在任何时间运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```
