---
layout: global
title: Aliyun Object Storage Service
nickname: 阿里云对象存储服务
group: Storage Integrations
priority: 10
---

* Table of Contents
{:toc}

本指南介绍了如何配置[阿里云 OSS](https://intl.aliyun.com/product/oss) 将其作为Alluxio 的底层存储系统。对象存储服务（Object Storage Service, OSS）是阿里云提供的海量、安全、高可靠的云存储服务。

## 部署条件

电脑上应已安装好 Alluxio 程序。如果没有安装，可[编译Alluxio源代码]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}), 或在[本地下载Alluxio程序]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

在将 OSS 与 Alluxio 一起运行前，请参照 [OSS 快速上手指南](https://www.alibabacloud.com/help/doc-detail/31883.htm)注册 OSS 或创建一个 OSS bucket。


## 基本设置

如果要使用OSS作为 Alluxio的底层存储，需要通过修改 `conf/alluxio-site.properties` 来配置Alluxio。如果该配置文件不存在，可通过模板创建。

```
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

编辑 `conf/alluxio-site.properties` 文件，将底层存储地址设置为 OSS bucket 和要挂载到 Alluxio 的 OSS 目录。例如，如果要将整个 bucket 挂载到 Alluxio，底层存储地址可以是 `oss://alluxio-bucket/` ，如果将名为 alluxio-bucket、目录为 `/alluxio/data` 的 OSS bucket 挂载到 Alluxio，则底层存储地址为 `oss://alluxio-bucket/alluxio/data`。

```
alluxio.master.mount.table.root.ufs=oss://<OSS_BUCKET>/<OSS_DIRECTORY>
``` 

指定访问 OSS 的阿里云凭证。在 `conf/alluxio-site.properties` 中，添加：

```
fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID>
fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET>
fs.oss.endpoint=<OSS_ENDPOINT>
```

`fs.oss.accessKeyId` 和 `fs.oss.accessKeySecret` 是 OSS 的 [AccessKey](https://www.alibabacloud.com/help/doc-detail/29009.htm)， 由[阿里云AccessKey管理工作台](https://ram.console.aliyun.com/)创建和管理。

`fs.oss.endpoint` 是这个bucket的网络端点 (endpoint)，见 bucket 概览页面，包含如 `oss-us-west-1.aliyuncs.com` 和 `oss-cn-shanghai.aliyuncs.com` 这样的值。可用的 endpoint 清单见
[OSS网络端点文档](https://intl.aliyun.com/help/doc-detail/31837.htm).

## 示例：将 Alluxio 与 OSS 一起在本地运行

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

访问 OSS 的目录 `oss://<OSS_BUCKET>/<OSS_DIRECTORY>` 以验证 Alluxio 创建的文件和目录是否存在。就本次测试而言，将看到如下的文件：`<OSS_BUCKET>/<OSS_DIRECTORY>/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

运行以下命令终止 Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 高级设置

### 嵌套挂载

OSS 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的
[Mount Command]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)（挂载命令）来进行挂载。例如：下述命令将 OSS bucket 里的一个目录挂载到 Alluxio 目录 `/oss`：

```console
$ ./bin/alluxio fs mount --option fs.oss.accessKeyId=<OSS_ACCESS_KEY_ID> \
  --option fs.oss.accessKeySecret=<OSS_ACCESS_KEY_SECRET> \
  --option fs.oss.endpoint=<OSS_ENDPOINT> \
  /oss oss://<OSS_BUCKET>/<OSS_DIRECTORY>/
```
