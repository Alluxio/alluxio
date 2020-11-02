---
layout: global
title: 在BOS上配置Alluxio
nickname: Alluxio使用BOS
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Baidu Cloud BOS](https://cloud.baidu.com/product/bos.html)作为底层文件系统。对象存储服务（BOS）是百度云提供的稳定、安全、高效、高可扩展的云存储服务。您可以将任意数量和形式的非结构化数据存入BOS，并对数据进行管理和处理。BOS支持标准、低频、冷存储等多种存储类型，满足您各类场景的存储需求。

## 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

另外，为了在BOS上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该指南中，BOS bucket的名称为BOS_BUCKET，在该bucket里的目录名称为BOS_DIRECTORY。另外，要使用BOS服务，还需提供一个bos 端点，该端点指定了你的bucket在哪个范围，本向导中的端点名为BOS_ENDPOINT。要了解更多指定范围的端点的具体内容，可以参考[这里](https://cloud.baidu.com/doc/Reference/Regions.html)，要了解更多BOS Bucket的信息，请参考[这里](https://cloud.baidu.com/doc/BOS/GettingStarted-new.html)

## 安装BOS

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。 BOS的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装

若要在Alluxio中使用BOS作为底层文件系统，一定要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的BOS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.underfs.address=bos://<BOS_BUCKET>/<BOS_DIRECTORY>/
```

接着，需要指定Baidu Cloud证书以便访问BOS，在`conf/alluxio-site.properties`中添加：

```
fs.bos.accessKeyId=<BOS_ACCESS_KEY_ID>
fs.bos.accessKeySecret=<BOS_ACCESS_KEY_SECRET>
fs.bos.endpoint=<BOS_ENDPOINT>
```

此处, `fs.bos.accessKeyId `和`fs.bos.accessKeySecret`分别为`Access Key ID`字符串[AccessKey](https://cloud.baidu.com/doc/Reference/GetAKSK.html)和`Access Key Secret`字符串，均受百度云[AccessKeys管理界面](https://console.bce.baidu.com/iam/#/iam/accesslist)管理；`fs.bos.endpoint`是Bucket概述中所说的Bucket的endpoint，其可能的取值比如`bj.bcebos.com`，`gz.bcebos.com`, `su.bcebos.com`。
([BOS Endpoint](https://cloud.baidu.com/doc/BOS/GettingStarted-new.html))。

更改完成后，Alluxio应该能够将BOS作为底层文件系统运行，你可以尝试[使用BOS在本地运行Alluxio](#使用bos在本地运行alluxio)

### 嵌套目录安装

BOS可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。 
[Mount 命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将BOS容器内部的目录挂载到Alluxio的`/bos`目录

```bash 
$ ./bin/alluxio fs mount --option fs.bos.accessKeyId=<BOS_ACCESS_KEY_ID> \
  --option fs.bos.accessKeySecret=<BOS_ACCESS_KEY_SECRET> \
  --option fs.bos.endpoint=<BOS_ENDPOINT> \
  /bos bos://<BOS_BUCKET>/<BOS_DIRECTORY>/
```

## 使用BOS在本地运行Alluxio

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

运行成功后，访问你的BOS目录`bos://<BOS_BUCKET>/<BOS_DIRECTORY>`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像`BOS_BUCKET/BOS_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`这样。。

运行以下命令停止Alluxio：

```bash
$ bin/alluxio-stop.sh local
```

