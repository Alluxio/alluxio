---
layout: global
title:远程登陆
group: Features
priority: 5
---

* 内容列表
{:toc}

## 概述
Alluxio支持通过网络向远程的日志服务器发送日志。这个特性对于必须完成日志收集任务的系统管理者来说十分有用。在远程登陆时，所有Alluxio服务器上的日志文件，例如master.log, worker
.log等，在日志服务器上的指定的且可配置的目录上将被轻松获取。

## 设置Alluxio
如要获取在集群上部署Alluxio的指导，请参考 [Running Alluxio on a cluster](Running-Alluxio-on-a-Cluster.html)

默认远程登陆是不可用的。如要使用远程登陆，您可以设置三个环境变量： `ALLUXIO_LOGSERVER_HOSTNAME`, `ALLUXIO_LOGSERVER_PORT` 和
`ALLUXIO_LOGSERVER_LOGS_DIR`。

日志服务器在何处运行并没有要求，只要其他Alluxio服务器对它可达。在例子中，我们在master所在的机器上运行日志服务器。

### 用环境变量使远程登陆可用
假设日志服务器的hostname是 `AlluxioLogServer`, 端口是 `45010`.
在 ./conf/alluxio-env.sh, 加入如下命令:

```bash
ALLUXIO_LOGSERVER_HOSTNAME=AlluxioLogServer
ALLUXIO_LOGSERVER_PORT=45010
ALLUXIO_LOGSERVER_LOGS_DIR=/tmp/alluxio_remote_logs
```

## 重启Alluxio和日志服务器
对参数进行修改后，首先您需要重启日志服务器，然后重启Alluxio。这样可以确保Alluxio在当前阶段生成的日志也能到达日志服务器。

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

第二步，进入已经设置好的日志服务器存储其他Alluxio服务器的日志的目录下。在上述例子中，这个目录是 `/tmp/alluxio_remote_logs`。

```bash
$ cd /tmp/alluxio_remote_logs
$ ls
master          proxy           secondary_master    worker
$ ls -l master/
...
-rw-r--r--  1 alluxio  alluxio  26109 Sep 13 08:49 34.204.198.64.log
...
```

您可以看到，日志文件根据类型被放入不同的文件夹中。Master logs放在文件夹  `master`, worker logs 放在文件夹 `worker`,
等等。在每个文件夹中，不同worker的日志文件通过服务器所在的机器的IP/hostname 来区分。
