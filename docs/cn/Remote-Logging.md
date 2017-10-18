---
layout: global
title:远程记录日志
group: Features
priority: 5
---

* 内容列表
{:toc}

## 概述
Alluxio支持通过网络向远程的日志服务器发送日志。这个特性对于必须完成日志收集任务的系统管理者来说十分有用。得益于远程日志汇总功能，所有Alluxio服务器上的日志文件，例如master.log, worker
.log等，可以在日志服务器上的指定路径轻松获取，且该路径可配置。

## 设置Alluxio
如要获取在集群上部署Alluxio的指导，请参考 [Running Alluxio on a cluster](Running-Alluxio-on-a-Cluster.html)

在默认情况下，远程日志汇总功能未被开启。如果要使用该功能，您可以设置三个环境变量： `ALLUXIO_LOGSERVER_HOSTNAME`, `ALLUXIO_LOGSERVER_PORT` 和
`ALLUXIO_LOGSERVER_LOGS_DIR`。

Alluxio对于日志服务器在何处运行并没有要求，只要其它Alluxio服务器可以访问它即可。在例子中，我们在master所在的机器上运行日志服务器。

### 用环境变量使远程登陆可用
假设日志服务器的hostname是 `AlluxioLogServer`, 端口是 `45010`.
在 ./conf/alluxio-env.sh, 加入如下命令:

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

## 重启Alluxio和日志服务器
对参数进行修改后，首先您需要重启日志服务器，然后重启Alluxio。这样可以确保Alluxio在启动阶段生成的日志也可以汇总到日志服务器。

### 启动日志服务器
在日志服务器，运行下列命令
```bash
$ ./bin/alluxio-start.sh logserver
```

### 启动Alluxio
在Alluxio master上, 运行下列命令
```bash
$ ./bin/alluxio-start.sh all
```

## 验证日志服务器已经启动
首先SSH登陆到日志服务器运行的机器上。

第二步，进入之前设置好的日志服务器存储远程日志的目录。在上述例子中，这个目录是 `/tmp/alluxio_remote_logs`。

```bash
$ cd /tmp/alluxio_remote_logs
$ ls
master          proxy           secondary_master    worker
$ ls -l master/
...
-rw-r--r--  1 alluxio  alluxio  26109 Sep 13 08:49 34.204.198.64.log
...
```

可以看到，日志文件根据类型被放入不同的文件夹中。Master logs放在文件夹  `master`, worker logs 放在文件夹 `worker`,
等等。在每个文件夹中，不同服务器的日志文件通过服务器所在的机器的IP/hostname 来区分。
