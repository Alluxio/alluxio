---
layout: global
title: Alluxio集成COS作为底层存储
nickname: Alluxio集成COS作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用腾讯云[对象存储](https://cloud.tencent.com/product/cos)(Cloud Object Storage，简称：COS)作为底层文件系统。对象存储是腾讯云提供的面向非结构化数据，支持 HTTP/HTTPS协议访问的分布式存储服务，它能容纳海量数据并保证用户对带宽和容量扩充无感知，可以作为大数据计算与分析的数据池。

## 声明
COS UFS存在读写性能较差以及不稳定的问题，为了解决此类问题，社区贡献了[COSN UFS](COSN.html)底层文件系统。COS和COSN UFS都是用于访问腾讯云对象存储，COSN相对于COS做了深度优化，其读写性能较COS成倍提升，同时带来了更好的稳定性，所以强烈推荐使用COSN。COS UFS将于Alluxio2.6.0版本停止维护。

## 初始步骤

通常，Alluxio以集群模式在多个机器上运行。需要在机器上部署二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)。

为了在COS上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该指南中，COS Bucket的名称为`COS_ALLUXIO_BUCKET`（表示COS的BUCKET_NAME,未包含appId信息，更详细使用说明可[参考这里](ttps://cloud.tencent.com/document/product/589/35283)），在该bucket里的目录名称为`COS_DATA`。还需提供一个COS的APPID与REGION，它们指定了你的bucket在哪个范围，本向导中的APPID名为`COS_APP_ID`, REGION名为`COS_REGION`。要了解更关于COS的信息，[请参考这里](https://cloud.tencent.com/document/product/436/7751)。

## 安装COS

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。 COS的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装

若要在Alluxio中使用COS作为底层文件系统，需修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的COS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.master.mount.table.root.ufs=cos://COS_ALLUXIO_BUCKET/COS_DATA/
```

接着，需要指定COS的配置信息以便访问COS，在`conf/alluxio-site.properties`中添加：

```
fs.cos.access.key=<COS_SECRET_ID>
fs.cos.secret.key=<COS_SECRET_KEY>
fs.cos.region=<COS_REGION>
fs.cos.app.id=<COS_APP_ID>
```

更改完成后，Alluxio应该能够将COS作为底层文件系统运行，你可以尝试[使用COS在本地运行Alluxio](#使用COS在本地运行Alluxio)

### 嵌套目录安装

COS可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。 [Mount命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将COS实例内部的目录挂载到Alluxio的/cos目录

```console
$ ./bin/alluxio fs mount --option fs.cos.access.key=<COS_SECRET_ID> \
    --option fs.cos.secret.key=<COS_SECRET_KEY> \
    --option fs.cos.region=<COS_REGION> \
    --option fs.cos.app.id=<COS_APP_ID> \
    /cos cos://<COS_ALLUXIO_BUCKET>/<COS_DATA>/
```

## 使用COS在本地运行Alluxio

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

运行成功后，访问你的COS目录`COS_ALLUXIO_BUCKET/COS_DATA`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像：

```console
COS_ALLUXIO_BUCKET/COS_DATA/default_tests_files/BASIC_CACHE_THROUGH
```

运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```
