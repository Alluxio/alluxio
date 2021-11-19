---
layout: global
title: Alluxio集成OBS作为底层存储
nickname: Alluxio集成OBS作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Huawei OBS](http://www.huaweicloud.com/en-us/product/obs.html)作为底层文件系统。对象存储服务（OBS）是华为云提供的一个大容量、安全、高可靠性的云存储服务。

## 前置条件

要运行 Alluxio 集群，需要在这些机器上部署二进制包。

在连接 `OBS` 到 Alluxio 前，OBS上需要有一个 bucket 及目录，如果不存在请创建它们。可以到[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0046535383.html)参考更多关于在 OBS 上创建 bucket 的信息。在本篇介绍中，bucket 名称取名为  `OBS_BUCKET `,目录名称取名为  `OBS_DIRECTORY`。

需提供一个 `OBS` 终端`OBS_ENDPOINT`,该终端用于声明bucket所在范围并且需要在 Alluxio 配置文件中设置。要了解更多关于 OBS 上不同范围和终端的信息可以参考[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html)

Alluxio 也同样支持 `PFS`（并行文件系统），这是 OBS 的扩展文件系统。请在下一章节中查看配置方式。

## 初始步骤

若要在 Alluxio 中使用 OBS 作为底层文件系统，需要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的OBS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.master.mount.table.root.ufs=obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

接着，需要指定华为云秘钥以便访问OBS，在 `conf/alluxio-site.properties` 中添加：

```
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
fs.obs.endpoint=<OBS_ENDPOINT>
```

此处, `fs.obs.accessKey`和`fs.obs.SecretKey`分别为`Access Key`字符串和`Secret Key`字符串，具体关于管理access key的信息可参考[这里](http://support.huaweicloud.com/en-us/usermanual-ca/en-us_topic_0046606340.html)。`fs.obs.endpoint`是Bucket概述中所说的 Bucket 的 endpoint，具体信息可参考[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html)。

如果你想使用 `PFS` 并行文件系统，需要在 `conf/alluxio-site.properties` 中如下配置，其余和 OBS 的配置相同：

```
# 默认值为 obs
fs.obs.bucketType=pfs
```



## 使用 OBS 在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master UI。

接着，你可以运行一个简单的示例程序：

```console
$ ./bin/alluxio runTests
```

运行成功后，访问你的OBS目录`obs://<OBS_BUCKET>/<OBS_DIRECTORY>`，确认其中包含了由Alluxio创建的文件和目录。

运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 嵌套目录挂载

`OBS` 可以安装在 Alluxio 命名空间中的嵌套目录中，以统一访问多个存储系统。[Mount 命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将 `OBS` 容器内部的目录挂载到 Alluxio 的 `/obs` 目录：

```console
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

