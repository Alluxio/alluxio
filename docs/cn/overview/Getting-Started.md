---
layout: global
title: 快速上手指南
group: Overview
priority: 1
---

* 内容列表
{:toc}
在这个快速上手指南里，我们会引导你在本地机器上安装 Alluxio。
快速上手指南包括以下任务：

* 下载和配置 Alluxio
* 验证 Alluxio 运行环境
* 本地启动 Alluxio
* 通过 Alluxio Shell 进行基本的文件操作
* **[加分项]** 挂载一个公开的 Amazon S3 bucket 到 Alluxio 上
* 关闭 Alluxio

**[加分项]** 如果你有一个[包含 access key id 和 secret accsee key 的 AWS 账户](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html)，你可以完成额外的任务。需要你 AWS 账户信息的章节都有**[加分项]**这一标签。

**注意** 本指南旨在让你快速开始与 Alluxio 系统进行交互。Alluxio 在运行大数据工作负载的分布式环境中表现最好。这些特性都难以适用于本地环境。如果你有兴趣运行一个更大规模的、能够突出 Alluxio 性能优势的例子，可以请求一个[沙箱集群](https://www.alluxio.io/sandbox-request)来一键免费部署Spark、Alluxio和S3。

## 前期准备

为了接下来的快速上手指南，你需要：

* Mac OS X 或 Linux
* [Java 8 或更新版本](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* **[加分项]** AWS 账户和密钥

### 安装 SSH (Mac OS X)

如果你使用 Mac OS X，你必须能够 ssh 到 localhost。远程登录开启方法：打开**系统偏好设置**，然后打开**共享**，确保**远程登录**已开启。

## 下载 Alluxio

首先，从[这里](http://www.alluxio.io/download)下载 Alluxio。

接着，你可以用如下命令解压下载包。

```console
$ tar -xzf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_VERSION_STRING}}
```

这会创建一个包含所有的 Alluxio 源文件和 Java 二进制文件的文件夹`alluxio-{{site.ALLUXIO_VERSION_STRING}}`。在本教程中，这个文件夹的路径将被引用为`${ALLUXIO_HOME}`。

## 配置 Alluxio

在`${ALLUXIO_HOME}/conf`目录下，根据模板文件创建`conf/alluxio-site.properties`配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

在`conf/alluxio-site.properties`文件中将 `alluxio.master.hostname`设置为 `localhost`。

```console
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```

### [加分项] AWS 相关配置

为了配置 Alluxio 与 Amazon S3 交互，请在`conf/alluxio-site.properties`文件中向 Alluxio 配置添加 AWS 访问信息。以下命令将更新该配置。

```console
$ echo "aws.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "aws.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

你必须将**`<AWS_ACCESS_KEY_ID>`**替换成你的 AWS access key id，将**`<AWS_SECRET_ACCESS_KEY>`**替换成你的 AWS secret access key。

## 验证 Alluxio 运行环境

在启动 Alluxio 前，我们要保证当前系统环境下 Alluxio 可以正常运行。我们可以通过运行如下命令来验证 Alluxio 的本地运行环境：

```console
$ ./bin/alluxio validateEnv local
```

该命令将汇报在本地环境运行 Alluxio 可能出现的问题。

你可以在[这里]({{ '/en/operation/User-CLI.html' | relativize_url }})查看更多关于`validateEnv`命令的信息。

## 启动 Alluxio

在启动 Alluxio 进程前，需要进行格式化。如下命令会格式化 Alluxio 的日志和 worker 存储目录。

```console
$ ./bin/alluxio format
```

默认配置下，本地运行 Alluxio 会启动一个 master 和一个 worker。我们可以用如下命令在 localhost 启动 Alluxio：

```console
$ ./bin/alluxio-start.sh local SudoMount
```

恭喜！Alluxio 已经启动并运行了！你可以访问 [http://localhost:19999](http://localhost:19999) 查看 Alluxio master 的运行状态，访问 [http://localhost:30000](http://localhost:30000) 查看 Alluxio worker 的运行状态。

## 使用 Alluxio Shell

[Alluxio shell]({{ '/en/operation/User-CLI.html' | relativize_url }}) 包含多种与 Alluxio 交互的命令行操作。如果要查看文件系统操作命令列表，运行：

```console
$ ./bin/alluxio fs
```

你可以通过`ls`命令列出 Alluxio 里的文件。比如列出根目录下所有文件：

```console
$ ./bin/alluxio fs ls /
```

目前 Alluxio 里没有文件。`copyFromLocal`命令可以拷贝本地文件到 Alluxio 中。

```console
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

再次列出 Alluxio 里的文件，可以看到刚刚拷贝的`LICENSE`文件：

```console
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     26847 NOT_PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
```

输出显示 LICENSE 文件在 Alluxio 中，也包含一些其他的有用信息，比如文件的大小、创建的日期、文件的所有者和组以及 Alluxio 中这个文件的缓存占比。

`cat`命令可以打印文件的内容。

```console
$ ./bin/alluxio fs cat /LICENSE
                                Apache License
                          Version 2.0, January 2004
                       http://www.apache.org/licenses/

  TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

默认设置中，Alluxio 使用本地文件系统作为底层文件系统 (UFS)。默认的 UFS 路径是`./underFSStorage`。我们可以查看 UFS 中的内容：

```console
$ ls ./underFSStorage/
```

然而，这个目录不存在！这是由于 Alluxio 默认只写入数据到 Alluxio 存储空间，而不会写入 UFS。

但是，我们可以告诉 Alluxio 将文件从 Alluxio 空间持久化到 UFS。shell 命令`persist`可以做到。

```console
$ ./bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

如果我们现在再次检查 UFS，文件就会出现。

```console
$ ls ./underFSStorage
LICENSE
```

如果我们在 [master webUI](http://localhost:19999/browse) 中浏览 Alluxio 文件系统，我们可以看见 LICENSE 文件以及其它有用的信息。其中，**Persistence State** 栏显示文件为 **PERSISTED**。

## [加分项] Alluxio 中的挂载功能

Alluxio 通过统一命名空间的特性统一了对存储系统的访问。你可以阅读[统一命名空间的博客](https://www.alluxio.io/resources/whitepapers/unified-namespace-allowing-applications-to-access-data-anywhere/)和[统一命名空间文档]({{ '/cn/core-services/Unified-Namespace.html' | relativize_url }})获取更详细的解释。

这个特性允许用户挂载不同的存储系统到 Alluxio 命名空间中并且通过 Alluxio 命名空间无缝地跨存储系统访问文件。

首先，我们在 Alluxio 中创建一个目录作为挂载点。

```console
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

接着，我们挂载一个已有的 S3 bucket 到 Alluxio。本指南使用`alluxio-quick-start`S3 bucket。

```console
$ ./bin/alluxio fs mount --readonly alluxio://localhost:19998/mnt/s3 s3://alluxio-quick-start/data
Mounted s3://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

我们可以通过 Alluxio 命名空间列出 S3 中的文件。使用熟悉的`ls`命令列出 S3 挂载目录下的文件。

```console
$ ./bin/alluxio fs ls /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

新挂载的文件和目录也可以在 [Alluxio web UI](http://localhost:19999/browse?path=%2Fmnt%2Fs3) 中看到。

通过 Alluxio 统一命名空间，你可以无缝地从不同存储系统中交互数据。举个例子，使用`ls -R`命令，你可以递归地列举出一个目录下的所有文件。

```console
$ ./bin/alluxio fs ls -R /
-rw-r--r-- staff  staff     26847 PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
drwxr-xr-x staff  staff         1 PERSISTED 01-09-2018 16:05:59:547  DIR /mnt
dr-x------ staff  staff         4 PERSISTED 01-09-2018 16:34:55:362  DIR /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

输出显示了 Alluxio 文件系统根目录下来源于挂载存储系统的所有文件。`/LICENSE`文件在本地文件系统中，`/mnt/s3/`目录在 S3 中。

## [加分项] 用 Alluxio 加速数据访问

由于 Alluxio 利用内存存储数据，它可以加速数据的访问。首先，我们看一看之前从 S3 挂载到 Alluxio 中的一个文件的状态：

```console
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

输出显示了文件 **Not In Memory**（不在内存中）。该文件是推特的样本。我们统计一下有多少推文提到了单词“kitten”，并计算该操作的耗时。

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c kitten
889

real	0m22.857s
user	0m7.557s
sys	0m1.181s
```

取决于你的网络连接状况，该操作可能会超过20秒。如果读取文件时间过长，你可以选择一个小一点的数据集。该目录下的其他文件是该文件的更小子集。
通过将数据放在内存中，Alluxio 可以提高访问该数据的速度。

在通过`cat`命令获取文件后，你可以用`ls`命令查看文件的状态：

```console
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002 100% /mnt/s3/sample_tweets_150m.csv
```

输出显示文件已经 100% 被加载到 Alluxio 中，既然如此，那么再次访问该文件的速度应该会快很多。

现在让我们来统计一下拥有“puppy”这个单词的推文数目。

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

如你所见，因为数据已经存放到了 Alluxio 内存中了，后续读这个相同文件的速度非常快。

现在让我们来统计一下有多少推文包含“bunny”这个词。

```console
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

恭喜！你在本地安装了 Alluxio 并且通过 Alluxio 加速了数据访问！

## 关闭 Alluxio

你可以使用如下命令关闭 Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 总结

恭喜你完成了 Alluxio 的快速上手指南！你成功地在本地电脑上下载和安装 Alluxio，并且通过 Alluxio shell 进行了基本的交互。这是一个如何上手 Alluxio 的简单例子。

除此之外还有很多可以学习的内容。你可以通过我们的文档学到 Alluxio 的各种特性。下面的资源详细介绍了 Alluxio 各种部署方式、如何挂载现有存储系统，以及如何配置现有应用程序与 Alluxio 交互。

### 部署 Alluxio

Alluxio 可以部署在很多不同的环境下。

* [本地运行 Alluxio]({{ '/en/deploy/Running-Alluxio-Locally.html' | relativize_url }})
* [在集群上独立运行 Alluxio]({{ '/en/deploy/Running-Alluxio-On-a-Cluster.html' | relativize_url }})
* [在 Docker 上运行 Alluxio]({{ '/en/deploy/Running-Alluxio-On-Docker.html' | relativize_url }})

### 底层存储系统

有很多可以通过 Alluxio 访问的底层存储系统。

* [Alluxio 使用 Azure Blob Store]({{ '/en/ufs/Azure-Blob-Store.html' | relativize_url }})
* [Alluxio 使用 S3]({{ '/en/ufs/S3.html' | relativize_url }})
* [Alluxio 使用 GCS]({{ '/en/ufs/GCS.html' | relativize_url }})
* [Alluxio 使用 Minio]({{ '/en/ufs/Minio.html' | relativize_url }})
* [Alluxio 使用 CephFS]({{ '/en/ufs/CephFS.html' | relativize_url }})
* [Alluxio 使用 CephObjectStorage]({{ '/en/ufs/CephObjectStorage.html' | relativize_url }})
* [Alluxio 使用 Swift]({{ '/en/ufs/Swift.html' | relativize_url }})
* [Alluxio 使用 GlusterFS]({{ '/en/ufs/GlusterFS.html' | relativize_url }})
* [Alluxio 使用 HDFS]({{ '/en/ufs/HDFS.html' | relativize_url }})
* [Alluxio 使用 OSS]({{ '/en/ufs/OSS.html' | relativize_url }})
* [Alluxio 使用 NFS]({{ '/en/ufs/NFS.html' | relativize_url }})

### 计算框架和应用

不同的计算框架和应用与 Alluxio 的集成。

* [Apache Spark 使用 Alluxio]({{ '/en/compute/Spark.html' | relativize_url }})
* [Apache Hadoop MapReduce 使用 Alluxio]({{ '/en/compute/Hadoop-MapReduce.html' | relativize_url }})
* [Apache HBase 使用 Alluxio]({{ '/en/compute/HBase.html' | relativize_url }})
* [Apache Hive 使用 Alluxio]({{ '/en/compute/Hive.html' | relativize_url }})
* [Presto 使用 Alluxio]({{ '/en/compute/Presto.html' | relativize_url }})
