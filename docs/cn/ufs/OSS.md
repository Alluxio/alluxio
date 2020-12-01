---
layout: global
title: Alluxio集成OSS作为底层存储
nickname: Alluxio集成OSS作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en)作为底层文件系统。对象存储服务（OSS）是阿里云提供的一个大容量、安全、高可靠性的云存储服务。

## 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

另外，为了在OSS上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该指南中，OSS bucket的名称为OSS_BUCKET，在该bucket里的目录名称为OSS_DIRECTORY。另外，要使用OSS服务，还需提供一个oss 端点，该端点指定了你的bucket在哪个范围，本向导中的端点名为OSS_ENDPOINT。要了解更多指定范围的端点的具体内容，可以参考[这里](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region)，要了解更多OSS Bucket的信息，请参考[这里](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## 安装OSS

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。 OSS的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装

若要在Alluxio中使用OSS作为底层文件系统，一定要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的OSS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.master.mount.table.root.ufs=oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```

接着，需要指定Aliyun证书以便访问OSS，在`conf/alluxio-site.properties`中添加：

```
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

此处, `fs.oss.accessKeyId `和`fs.oss.accessKeySecret`分别为`Access Key ID`字符串和`Access Key Secret`字符串，均受阿里云[AccessKeys管理界面](https://ak-console.aliyun.com)管理；`fs.oss.endpoint`是Bucket概述中所说的Bucket的endpoint，其可能的取值比如`oss-us-west-1.aliyuncs.com `，`oss-cn-shanghai.aliyuncs.com`。
([OSS Internet Endpoint](https://intl.aliyun.com/help/doc-detail/31837.htm))。

更改完成后，Alluxio应该能够将OSS作为底层文件系统运行，你可以尝试[使用OSS在本地运行Alluxio](#使用OSS在本地运行Alluxio)

### 嵌套目录安装

OSS可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。 
[Mount 命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将OSS容器内部的目录挂载到Alluxio的`/oss`目录

```console 
$ ./bin/alluxio fs mount --option fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> \
  --option fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET> \
  --option fs.oss.endpoint=<OSS_ENDPOINT> \
  /oss oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```

## 使用OSS在本地运行Alluxio

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

运行成功后，访问你的OSS目录`oss://<OSS_BUCKET>/<OSS_DIRECTORY>`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像`OSS_BUCKET/OSS_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`这样。。

运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

