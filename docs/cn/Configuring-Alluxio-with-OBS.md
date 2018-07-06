---
layout: global
title: 在OBS上配置Alluxio
nickname: Alluxio使用OBS
group: Under Store
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Huawei OBS](http://www.huaweicloud.com/en-us/product/obs.html)作为底层文件系统。对象存储服务（OBS）是华为云提供的一个大容量、安全、高可靠性的云存储服务。

## 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制包。你可以自己[编译Alluxio](http://alluxio.org/documentation/master/Building-Alluxio-Master-Branch.html)，或者[下载二进制包](http://alluxio.org/documentation/master/Running-Alluxio-Locally.html)。

OBS底层存储系统可作为扩展实现。预编译的OBS底层存储jar包可以从[这里](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs/target)下载。

然后在master节点上执行以下命令在`conf/masters`和`conf/workers`中定义的所有master节点和worker节点上安装扩展版本：

```bash
$ bin/alluxio extensions install /PATH/TO/DOWNLOADED/OBS/jar
```

了解更多Alluxio扩展管理信息请参考[这里](UFSExtensions.html) 

在连接OBS到Alluxio前，OBS上需要有一个bucket及目录，如果不存在请创建它们。建议bucket命名为`OBS_BUCKET`,目录命名为`OBS_DIRECTORY`。可以到[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0046535383.html)参考更多关于在OBS上创建bucket的信息。

需提供一个OBS终端建议命名为`OBS_ENDPOINT`,该终端用于声明bucket所在范围并且需要在Alluxio配置文件中设置。要了解更多关于OBS上不同范围和终端的信息可以参考[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html)

## 登录OBS

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。OBS的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装
若要在Alluxio中使用OBS作为底层文件系统，需要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的OBS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.underfs.address=obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

接着，需要制定华为云证书以便访问OBS，在`conf/alluxio-site.properties`中添加：

```
fs.obs.accessKey=<OBS_ACCESS_KEY>
fs.obs.secretKey=<OBS_SECRET_KEY>
fs.obs.endpoint=<OBS_ENDPOINT>
```

此处, `fs.obs.accessKey`和`fs.obs.SecretKey`分别为`Access Key`字符串和`Secret Key`字符串，具体关于管理access key的信息可参考[这里](http://support.huaweicloud.com/en-us/usermanual-ca/en-us_topic_0046606340.html)。`fs.obs.endpoint`是Bucket概述中所说的Bucket的endpoint，具体信息可参考[这里](http://support.huaweicloud.com/en-us/qs-obs/en-us_topic_0075679174.html)。

更改完成后，Alluxio应该能够将OBS作为底层文件系统运行，你可以尝试使用OBS在本地运行Alluxio。

### 嵌套目录安装

OBS可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。[Mount 命令](Command-Line-Interface.html#mount)可以实现这一目的。例如，下面的命令将OBS容器内部的目录挂载到Alluxio的`/obs`目录：

```bash
$ ./bin/alluxio fs mount --option fs.obs.accessKey=<OBS_ACCESS_KEY> \
  --option fs.obs.secretKey=<OBS_SECRET_KEY> \
  --option fs.obs.endpoint=<OBS_ENDPOINT> \
  /obs obs://<OBS_BUCKET>/<OBS_DIRECTORY>/
```

## 使用OBS在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master UI。

接着，你可以运行一个简单的示例程序：

```bash
$ bin/alluxio runTests
```

运行成功后，访问你的OBS目录`obs://<OBS_BUCKET>/<OBS_DIRECTORY>`，确认其中包含了由Alluxio创建的文件和目录。

运行以下命令停止Alluxio：

```bash
$ bin/alluxio-stop.sh local
```
