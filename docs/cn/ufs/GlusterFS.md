---
layout: global
title: Alluxio集成GlusterFS作为底层存储
nickname: Alluxio集成GlusterFS作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[GlusterFS](http://www.gluster.org/)作为底层文件系统。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Alluxio

你需要通过修改`conf/alluxio-site.properties`来配置Alluxio使用底层存储系统，如果该配置文件不存在，则根据模版创建一个配置文件

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

假定GlusterFS bricks与Alluxio部署在同样的节点上，且GlusterFS volume挂载在`/mnt/gluster`，那以下的环境变量要添加到`conf/alluxio-site.properties`配置文件中：

```properties
alluxio.master.mount.table.root.ufs=/mnt/gluster
```

## 使用GlusterFS在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察是否一切运行正常

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

```console
$ ./bin/alluxio runTests
```

运行成功后，访问你的GlusterFS volume，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

```
/mnt/gluster/default_tests_files/Basic_CACHE_THROUGH
```

运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```
