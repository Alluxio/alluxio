---
layout: global
title: Quick Start Guide
---

本指南将介绍如何在本地计算机上快速运行 Alluxio。
指南将涵盖以下任务：

* 下载并配置 Alluxio
* 在本地启动 Alluxio
* 通过 Alluxio Shell 执行基本任务
* **[额外]** 在 Alluxio 中挂载公共 Amazon S3 存储桶
* **[额外]** 在 Alluxio 的存储下挂载 HDFS
* 停止 Alluxio


本指南包含标有 **[额外]** 的可选任务，这些任务使用来自
[具有访问密钥 id 和秘密访问密钥的 AWS 账户］(http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html)。

▶️ [不到 3 分钟即可启动并运行 Alluxio！](https://youtu.be/5YQvvznT5cI){:target="_blank"} (2:36)

**备注**: 本指南旨在以最少的设置在单台机器上启动 Alluxio 系统。
如果要加快 SQL 分析速度，可以尝试使用[Presto Alluxio 入门](https://www.alluxio.io/alluxio-presto-sandbox-docker/) 教程。

## 环境
* MacOS or Linux
* [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* 启用远程登录：请参阅[MacOS 用户须知](http://osxdaily.com/2011/09/30/remote-login-ssh-server-mac-os-x/)
* **[额外]** AWS 账户和密钥

## 下载 Alluxio

从 [此页] 下载 Alluxio(https://alluxio.io/downloads/). 选择所需的版本，然后是为默认 Hadoop 构建的发行版。
使用以下命令解压下载的文件。

```shell
$ tar -xzf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_VERSION_STRING}}
```

这将创建一个目录`alluxio-{{site.ALLUXIO_VERSION_STRING}}`，其中包含所有 Alluxio
源文件和 Java 二进制文件。在本教程中，该目录的路径将被称为
称为 `${ALLUXIO_HOME}`。

## 配置 Alluxio

在 `${ALLUXIO_HOME}/conf` 目录中，复制模板文件，创建 `conf/alluxio-env.sh` 配置文件。
复制模板文件，创建 `conf/alluxio-env.sh` 配置文件。

```shell
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```

在 `conf/alluxio-env.sh` 中，为 `JAVA_HOME` 添加配置。例如

```shell
$ echo "JAVA_HOME=/path/to/java/home" >> conf/alluxio-env.sh
```

在 `${ALLUXIO_HOME}/conf` 目录中，通过复制模板文件创建配置文件 `conf/alluxio-site.properties
复制模板文件，创建 `conf/alluxio-site.properties` 配置文件。

```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

将 `conf/alluxio-site.properties` 中的 `alluxio.master.hostname` 设置为 `localhost`。

```shell
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

在 `conf/alluxio-site.properties` 中设置其他参数
```shell
$ echo "alluxio.dora.client.read.location.policy.enabled=true" >> conf/alluxio-site.properties
$ echo "alluxio.user.short.circuit.enabled=false" >> conf/alluxio-site.properties
$ echo "alluxio.master.worker.register.lease.enabled=false" >> conf/alluxio-site.properties
$ echo "alluxio.worker.block.store.type=PAGE" >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.type=LOCAL" >> conf/alluxio-site.properties >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.sizes=1GB" >> conf/alluxio-site.properties
$ echo "alluxio.worker.page.store.page.size=1MB" >> conf/alluxio-site.properties
```
将页面存储目录设置为当前用户拥有读/写权限的现有目录。
下面以 `/mnt/ramdisk` 为例。
```shell
$ echo "alluxio.worker.page.store.dirs=/mnt/ramdisk" >> conf/alluxio-site.properties
```
分页缓存存储指南]({{ '/en/core-services/Data-Caching.html | relativize_url }}#paging-worker-storage）有更多关于如何配置页面块存储的信息。

配置 Alluxio ufs：
```shell
$ echo "alluxio.dora.client.ufs.root=/tmp" >> conf/alluxio-site.properties
```

`<UFS_URI>` 应该是完整的 ufs uri。在单节点部署中，可将其设置为本地文件夹（如默认值 `/tmp`），或设置为整个 UFS uri。
或完整的 ufs uri（例如 `hdfs://namenode:port/path/` 或 `s3://bucket/path`）。


### [额外] AWS 配置
要配置 Alluxio 与 Amazon S3 交互，请在 `conf/alluxio-site.properties` 中的 Alluxio 配置中添加 AWS 访问信息。

```shell
$ echo "alluxio.dora.client.ufs.root=s3://<BUCKET_NAME>/<DIR>" >> conf/alluxio-site.properties
$ echo "s3a.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "s3a.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```
将`s3://<BUCKET_NAME>/<DIR>`、`<AWS_ACCESS_KEY_ID>`和`<AWS_SECRET_ACCESS_KEY>`分别替换为
分别加上有效的 AWS S3 地址、AWS 访问密钥 ID 和 AWS 密钥。

更多信息，请参阅[S3 配置文档]({{ '/en/ufs/S3.html' | relativize_url }})。

###[额外] HDFS 配置
要配置 Alluxio 与 HDFS 交互，请在 `conf/alluxio-site.properties` 中提供每个节点本地可用的 HDFS 配置文件的路径。

```shell
$ echo "alluxio.dora.client.ufs.root=hdfs://nameservice/<DIR>" >> conf/alluxio-site.properties
$ echo "alluxio.underfs.hdfs.configuration=/path/to/hdfs/conf/core-site.xml:/path/to/hdfs/conf/hdfs-site.xml" >> conf/alluxio-site.properties
```

替换 `nameservice/<DIR>` and `/path/to/hdfs/conf` 与实际值相一致。

更多信息，请参阅 [HDFS 配置文档]({{ '/en/ufs/HDFS.html' | relativize_url }})。

## 启动 Alluxio

启动程序前需要格式化 Alluxio。以下命令会格式化 Alluxio 日志和工作站存储目录。

```shell
$ ./bin/alluxio init format
```
启动 Alluxio 服务

```shell
$ ./bin/alluxio process start local
```

恭喜您 Alluxio 现已开始运行！

## 使用 Alluxio Shell

[Alluxio shell]({{ '/en/operation/User-CLI.html' | relativize_url }})提供与 Alluxio 交互的命令行操作。
命令行操作，以便与 Alluxio 交互。下面文件系统操作列表命令，可运行

```shell
$ ./bin/alluxio fs
```

使用 `ls` 命令列出 Alluxio 中的文件。要列出根目录下的所有文件，请使用
以下命令：

```shell
$ ./bin/alluxio fs ls /
```

目前，Alluxio 中没有文件。使用copyFromLocal` shell 命令将文件复制到 Alluxio 中。

```shell
$ ./bin/alluxio fs copyFromLocal ${ALLUXIO_HOME}/LICENSE /LICENSE
Copied file://${ALLUXIO_HOME}/LICENSE to /LICENSE
```

再次列出 Alluxio 中的文件，查看 `LICENSE` 文件。

```shell
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     27040     02-17-2021 16:21:11:061 0% /LICENSE
```
输出结果显示文件已成功写入 Alluxio 的存储空间。检查设置为 `alluxio.dora.client.ufs.root` 值的目录，默认为 `/tmp`。

```shell
$ ls /tmp
LICENSE
```
`cat` 命令打印文件内容。

```shell
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```
读取文件时，Alluxio 也会缓存该文件，以加快未来的数据访问速度。

## 停止 Alluxio

使用以下命令停止 Alluxio：

```shell
$ ./bin/alluxio process stop local
```

## 下一步动作

恭喜您启动了 Alluxio！本指南介绍了如何下载并在本地安装 Alluxio，以及通过 Alluxio shell 进行基本交互的示例。

接下来有几个步骤可供选择：
* 在我们的文档中了解有关 Alluxio 各项功能的更多信息，例如
数据缓存]({{ '/en/core-services/Data-Caching.html' | relativize_url }})和[元数据缓存]({{ '/en/core-services/Metadata-Caching.html' | relativize_url }})。
* 查看如何[安装具有高可用性（HA）的 Alluxio 群集]({{ '/en/deploy/Install-Alluxio-Cluster-with-HA.html' | relativize_url }})
* 您还可以使用 Alluxio K8s Helm Chart 或 Alluxio K8s Operator [Install Alluxio on Kubernetes]（{{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.html' | relativize_url }}）。
* 连接计算引擎，如 [Presto]（{{ '/en/compute/Presto.html' | relativize_url }}）、[Trino]（{{ '/en/compute/Trino.html' | relativize_url }}）或 [Apache Spark]（{{ '/en/compute/Spark.html' | relativize_url }})
* 连接文件存储，如[亚马逊 AWS S3]({{ '/en/ufs/S3.html' | relativize_url }})、[HDFS]({{ '/en/ufs/HDFS.html' | relativize_url }})或[谷歌云存储]({{ '/en/ufs/GCS.html' | relativize_url }})
* 如果你有兴趣成为贡献者，请查看我们的[贡献指南]({{ '/en/contributor/Contribution-Guide.html' | relativize_url }})！

## FAQ
### 为什么我在使用 ssh 和 alluxio 时总是收到 "操作不允许 "的提示？

对于使用 macOS 11（Big Sur）或更高版本的用户，当运行命令
```shell
$ ./bin/alluxio init format
```
您可能会收到错误信息：
```
alluxio-{{site.ALLUXIO_VERSION_STRING}}/bin/alluxio: Operation not permitted
```
这可能是 macOS 新添加的设置选项造成的。
要解决这个问题，请打开 "系统偏好设置 "并打开 "共享"。

![macOS系统偏好设置共享]({{ '/img/screenshot_sharing_setting.png' | relativize_url }})

在左侧选中 "远程登录 "旁边的复选框。如果有 "允许远程用户完全访问"，如图所示
图片所示，请选中其旁边的复选框。此外，单击 "+"按钮，将自己添加到允许远程登录的用户列表中。
允许远程登录的用户列表中。

## 调试

### 可选的 Dora 服务器端元数据缓存

默认情况下，Dora Worker 会缓存元数据和数据。将 "alluxio.dora.client.metadata.cache.enabled "设为 "false "可禁用元数据缓存。如果禁用，客户端将始终直接从存储中获取元数据。

### 通过 Netty 实现高性能数据传输

将 "alluxio.user.netty.data.transmission.enabled "设为 "true"，以启用通过 Netty 在客户端和 Dora 缓存节点之间传输数据。Dora 缓存节点之间通过 Netty 传输数据。这样就避免了 gRPC 的序列化和反序列化成本，并减少了工作站端的资源消耗。

## 已知限制

1. Dora 仅支持一个 UFS。暂不支持嵌套挂载。
2. Alluxio 主节点仍需正常运行。它用于发现 Dora Worker、
   集群配置更新，以及处理写 I/O 操作。


