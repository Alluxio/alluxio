---
layout: global
title: Tinder Object Storage Service
nickname: 火山云对象存储服务
group: Storage Integrations
priority: 10
---

* Table of Contents
  {:toc}

本指南介绍了如何配置[火山云 TOS](https://www.volcengine.com/product/TOS) 将其作为Alluxio 的底层存储系统。对象存储服务（Tinder Object Storage, TOS）是火山引擎提供的海量、安全、低成本、易用、高可靠、高可用的分布式云存储服务。

## 部署条件

电脑上应已安装好 Alluxio 程序。如果没有安装，可[编译Alluxio源代码]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), 或在[本地下载Alluxio程序]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

在将 TOS 与 Alluxio 一起运行前，请参照 [TOS 快速上手指南](https://www.volcengine.com/docs/6349/74830)注册 TOS 或创建一个 TOS bucket。


## 基本设置

如果要使用TOS作为 Alluxio的底层存储，需要通过修改 `conf/alluxio-site.properties` 来配置Alluxio。如果该配置文件不存在，可通过模板创建。

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

编辑 `conf/alluxio-site.properties` 文件，将底层存储地址设置为 TOS bucket 和要挂载到 Alluxio 的 TOS 目录。例如，如果要将整个 bucket 挂载到 Alluxio，底层存储地址可以是 `tos://alluxio-bucket/` ，如果将名为 alluxio-bucket、目录为 `/alluxio/data` 的 TOS bucket 挂载到 Alluxio，则底层存储地址为 `tos://alluxio-bucket/alluxio/data`。

```
alluxio.master.mount.table.root.ufs=tos://<TOS_BUCKET>/<TOS_DIRECTORY>
``` 

指定访问 TOS 的火山云云凭证。在 `conf/alluxio-site.properties` 中，添加：

```
fs.tos.accessKeyId=<TOS_ACCESS_KEY_ID>
fs.tos.accessKeySecret=<TOS_ACCESS_KEY_SECRET>
fs.tos.endpoint=<TOS_ENDPOINT>
fs.tos.region=<TOS_REGION>
```

`fs.tos.accessKeyId` 和 `fs.tos.accessKeySecret` 是 TOS 的 [AccessKey](https://www.volcengine.com/docs/6291/65568)， 由[TOS AccessKey管理工作台](https://console.volcengine.com/iam/keymanage/)创建和管理。

`fs.tos.endpoint` 是这个bucket的网络端点 (endpoint)，见 bucket 概览页面，包含如 `tos-cn-beijing.volces.com` 和 `tos-cn-guangzhou.volces.com` 这样的值。可用的 endpoint 清单见
[TOS网络端点文档](https://www.volcengine.com/docs/6349/107356).

`fs.tos.region` 是 bucket 所在的地域，如 `cn-beijing` 或 `cn-guangzhou`。可用的地域清单见[TOS地域文档](https://www.volcengine.com/docs/6349/107356)。

## 示例：将 Alluxio 与 TOS 一起在本地运行

启动 Alluxio 服务器：

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令会启动一个 Alluxio master 和一个 Alluxio worker。可通过 [http://localhost:19999](http://localhost:19999) 查看 master UI。

运行一个简单的示例程序：

```console
$ ./bin/alluxio runTests
```

访问 OSS 的目录 `tos://<TOS_BUCKET>/<TOS_DIRECTORY>` 以验证 Alluxio 创建的文件和目录是否存在。就本次测试而言，将看到如下的文件：`<TOS_BUCKET>/<TOS_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

运行以下命令终止 Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 高级设置

### 嵌套挂载

TOS 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的
[Mount Command]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)（挂载命令）来进行挂载。例如：下述命令将 TOS bucket 里的一个目录挂载到 Alluxio 目录 `/tos`：

```console
$ ./bin/alluxio fs mount --option fs.tos.accessKeyId=<TOS_ACCESS_KEY_ID> \
  --option fs.tos.accessKeySecret=<TOS_ACCESS_KEY_SECRET> \
  --option fs.tos.endpoint=<TOS_ENDPOINT> \
  /tos tos://<TOS_BUCKET>/<TOS_DIRECTORY>/
```

### TOS 流式上传

由于TOS作为对象存储的特性，文件上传时会被从客户端发送到Worker节点，并被存储在本地磁盘的临时目录中，默认在 close() 方法中被上传到TOS。

要启用流式上传，可以在 `conf/alluxio-site.properties` 中添加以下配置：

```
alluxio.underfs.tos.streaming.upload.enabled=true
```

默认上传过程更安全，但存在以下问题：

- 上传速度慢。文件必须先发送到 Alluxio worker，然后 Alluxio worker 负责将文件上传到 TOS。这两个过程是顺序执行的。

- 临时目录必须具有存储整个文件的容量。

- 如果上传过程中断，文件可能会丢失。

TOS流式上传功能解决了上述问题。

TOS流式上传功能的优点：

- 更短的上传时间。Alluxio worker 在接收新数据的同时上传缓冲数据。总上传时间至少与默认方法一样快。

- 容量要求更小，我们的数据是按照分片进行缓存和上传的（alluxio.underfs.tos.streaming.upload.partition.size默认是64MB），当一个分片上传成功后，这个分片就会被删除。

- 更快的 close() 方法：close() 方法执行时间大大缩短，因为文件的上传在写入过程中已经完成。

如果 TOS 流式上传中断，则可能会有中间分区上传到 TOS，并且 TOS 将对这些数据收费。

为了减少费用，用户可以修改conf/alluxio-site.properties包括：

```
alluxio.underfs.cleanup.enabled=true
```

当Alluxio检测达到清理间隔时，所有非只读 TOS 挂载点中超过清理年龄的中间分段上传都将被清理。

### 高并发调优

Alluxio 与 TOS 集成时，可以通过调整以下配置来优化性能：

- `alluxio.underfs.tos.retry.max`：用于控制与 TOS 的重试次数。默认值为 3。
- `alluxio.underfs.tos.read.timeout`：用于控制与 TOS 的读取超时时间。默认值为 30000 毫秒。
- `alluxio.underfs.tos.write.timeout`：用于控制与 TOS 的写入超时时间。默认值为 30000 毫秒。
- `alluxio.underfs.tos.streaming.upload.partition.size`：用于控制 TOS 流式上传的分区大小。默认值为 64MB。
- `alluxio.underfs.tos.connect.timeout`: 用于控制与 TOS 的连接超时时间。默认值为 30000 毫秒。

