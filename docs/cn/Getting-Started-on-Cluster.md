---
layout: global
title: Getting Started on a Cluster
group: User Guide
priority: 2
---

* Table of Contents
{:toc}

[快速上手指南](Getting-Started.html) 展示了如何在单机上部署本地版本的 Alluxio。
在这篇进阶文档中，我们将会在一个拥有三台 Linux 服务器的集群上安装部署 Alluxio、
将 AWS S3 bucket 配置成一个 UFS (under file system)，并且在 Alluxio 上执行一些简单的操作。

在这篇指南中，你将会：

* 在一个 Linux 集群上下载并且配置 Alluxio
* 配置 AWS
* 验证 Alluxio 的环境
* 配置一个分布式的存储作为 UFS
* 在多个节点上启动 Alluxio
* 通过 Alluxio Shell 执行简单的操作
* **[可选]** 在 Alluxio 集群上运行简单的 Hadoop MapReduce 任务
* **[可选]** 在 Alluxio 集群上运行简单的 Spark 任务
* 关闭 Alluxio

**[需求]** 在这篇指南中，我们会使用 AWS S3 作为 Alluxio 的示例 UFS。

请确保您有一个 [AWS 帐号并且创建了 access key id 和 secret access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys),
以便可以在接下来使用 S3 服务。如果您还没有一个 AWS 帐号，您也可以使用一个现有的 HDFS 集群作为 UFS，这样在 [配置分布式存储作为 UFS](#set-up-a-distributed-storage-as-ufs) 部分会有一些不同。

**注意** 本指南旨在带您快速地在分布式环境中创建一个可以交互的 Alluxio 环境。
如果您想要在一个更大规模的、更在乎 Alluxio 性能提升的系统，
可以参考这两本白皮书：[使用 Alluxio 加速按需数据分析](https://alluxio.com/resources/accelerating-on-demand-data-analytics-with-alluxio)，
和[使用 Alluxio 加速基于 Ceph 对象存储的数据分析](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio).

## 前提

对于下面的快速开始指南，你需要

* 拥有 3 个节点的 Linux 集群
* [Java 7 或更新版本](Java-Setup.html)
* AWS 帐号和 key

## 配置 SSH （Linux）

请确保所有节点上，ssh 已经启用

配置一个免密码的 ssh 能使得分布式的复制和配置更加便捷。在这篇文档中，我们采用手动下载并
在多个节点上启动 Alluxio 的方式，这仅仅作为一个演示。这里的操作过程可以简单的使用命令
`./alluxio copyDiy` 或者是 Ansible 脚本。可以参考[在集群上运行 Alluxio](Running-Alluxio-on-a-Cluster.html)
和 [在 EC2 上运行 Alluxio](Running-Alluxio-on-EC2.html)。

## 下载 Alluxio

首先，下载 Alluxio 到本地。你可以从[下载页面](http://www.alluxio.org/download) 下载支持不同版本 Hadoop 的预编译的 Alluxio 的最新版本（{{site.ALLUXIO_RELEASED_VERSION}}）。

然后，使用 `scp` 命令将压缩包拷贝到所有的节点上。可以使用自动化脚本（如Ansible）方案来
在多节点上分发压缩包和执行下述的过程。这篇文档展示了手动执行的命令，其中的大部分都需要在所有节点上执行。
列出原始的命令来帮助用户理解 Alluxio 的组件是如何独立启动的。

接下来，在所有节点上解压压缩包。根据你下载的版本不同，文件名会有些许不同。

```bash
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

这会创建一个目录 `alluxio-{{site.ALLUXIO_RELEASED_VERSION}}`，包括了 Alluxio 所有的脚本和二进制文件。

## 配置 Alluxio

在启动 Alluxio 之前，我们需要进行配置。请确保在所有节点上都执行了这些命令。

在 `${ALLUXIO_HOME}/conf` 目录中，按照配置文件模板创建配置文件 `conf/alluxio-site.properties` 。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

更新 `alluxio.master.hostname` 配置项为你想要运行 Alluxio Master 的节点的 hostname，该项在 `conf/alluxio-site.properties` 文件中。
假定它叫 Alluxio 主节点，以及它的 hostname 为 **${ALLUXIO_MASTER_HOSTNAME}**.

```bash
$ echo "alluxio.master.hostname=${ALLUXIO_MASTER_HOSTNAME}" >> conf/alluxio-site.properties
```

### 配置 AWS

请确保在所有节点上都执行了这些命令。

如果你有 Amazon AWS 帐号，且已经启用了 access key id 和 secret key，你可以更新 Alluxio 的配置文件，以便后续与 Amazon S3 进行交互。
在 `conf/alluxio-site.properties` 文件中添加你的 AWS 访问信息。通过以下命令来更新配置。

```bash
$ echo "aws.accessKeyId=${AWS_ACCESS_KEY_ID}" >> conf/alluxio-site.properties
$ echo "aws.secretKey=${AWS_SECRET_ACCESS_KEY}" >> conf/alluxio-site.properties
```

请用您自己的 AWS access key id 和 AWS secret key 来替换命令中的 **AWS_ACCESS_KEY_ID** 和**AWS_SECRET_ACCESS_KEY**。
至此，Alluxio 就配置好了。

## 验证 Alluxio 的环境

在启动 Alluxio 之前，可以通过以下命令来确保系统的环境和配置可以运行 Alluxio 服务。
在各个节点上执行以下命令来验证环境：

```bash
$ sudo bin/alluxio validateEnv local
```

你也可以只验证指定的验证项。例如：

```bash
$ sudo bin/alluxio validateEnv local ulimit
```

这条命令只会在特定节点上执行系统资源限制的验证任务。

你可以在这个[页面](Developer-Tips.html)上查看该命令的详细信息。

## 配置一个分布式的存储来作为 UFS

[快速开始指南](Getting-Started.html) 展示了在默认的本地文件系统上运行 Alluxio。
在分布式集群模式中，Alluxio 需要一个分布式的存储系统来作为底层的文件系统。
在本文档中，我们使用 AWS S3 来作为一个示例。如果你想要使用现有的 HDFS 作为 UFS，
请参考[这篇文档](Configuring-Alluxio-with-HDFS.html)。

创建一个新的 S3 bucket，或者使用现有的。选择一个 S3 bucket 的目录来作为底层存储。
在本指南中，我们假定这个 S3 bucket 的名字叫`S3_BUCKET`, 使用的目录为 `S3_DIRECTORY`.

通过修改配置文件 `conf/alluxio-site.properties` 来让 Alluxio 使用 AWS S3 作为底层存储系统。
首先要指定一个 **已经存在的** S3 bucket 和目录。这个修改要在所有节点上执行。
在配置文件 `conf/alluxio-site.properties` 中包括以下项：

```
alluxio.underfs.address=s3a://S3_BUCKET/S3_DIRECTORY
```

注意：在前文 `配置 AWS` 部分，需要的 AWS 认证信息已经在所有节点上配置好了，这保证了这些节点能够访问 S3 bucket。

如果你想启用 S3 UFS 的高级特性，可以参考[这篇文档](Configuring-Alluxio-with-S3.html).

## 启动 Alluxio

接着，我们需要在启动之前格式化 Alluxio。在主节点执行以下命令：

```bash
$ bin/alluxio format
```

现在，我们就可以启动 Alluxio 啦！在本文档中，我们启动一个 Alluxio Master 节点以及 两个 Alluxio Worker 节点。
在任一节点上，执行以下命令来启动 Alluxio 并作为 Master：

```bash
$ bin/alluxio-start.sh master
```

恭喜！Alluxio Master 现在启动运行了！你可以访问 http://ALLUXIO_MASTER_HOSTNAME:19999
来查看 Alluxio Master 的状态。

在另外两个节点上，运行以下命令来启动 Alluxio 并作为 Worker：

```bash
$ bin/alluxio-start.sh worker SudoMount
```

等待片刻，Alluxio Worker 会向 Alluxio Master 注册。
你可以访问 http://ALLUXIO_WORKER_HOSTNAME:30000 来查看 Alluxio Worker 的状态。


## 使用 Alluxio Shell

当 Alluxio 启动以后，我们可以使用 [Alluxio shell](Command-Line-Interface.html)
来测试 Alluxio 文件系统。Alluxio Shell 支持很多与 Alluxio 的交互操作。
你可以使用以下命令来启动 Alluxio Shell：

```bash
$ bin/alluxio fs
```

这条命令会打印出可用的 Alluxio 命令行操作。

例如，你可以使用 `ls` 命令来列出 Alluxio 中文件。使用以下命令来列出根目录下的所有文件：

```bash
$ bin/alluxio fs ls /
```

现在，Alluxio 中还没有任何文件。我么可拷贝一个文件到 Alluxio 中。
`copyFromLocal` 命令就是用来将本地文件拷贝到 Alluxio 中。

```bash
$ bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

执行上述命令，将 `LICENSE` 文件拷贝到 Alluxio 中，现在我们可以在 Alluxio 中看见它：

```bash
$ bin/alluxio fs ls /
-rw-r--r--   ubuntu    ubuntu    26.22KB   NOT_PERSISTED 09-22-2017 09:30:08:781  100%      /LICENSE
```

输出结果显示这个文件确实在 Alluxio 中了，并且包括了一些其他有用的信息，如文件大小、创建日期、缓存比例。

通过 `cat` 命令来查看文件内容：

```bash
$ bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

With this UFS configuration, Alluxio uses the specified S3 bucket as its under file system (UFS).

We can check whether this file exists in the S3 bucket. However, the directory doesn't exist on S3!
By default, Alluxio will write data only into Alluxio space, not to the UFS. However, we can tell
Alluxio to persist the file from Alluxio space to the UFS. The shell command `persist` will do just that.

```bash
$ bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

现在，如果我们再次检查 S3 bucket，这个文件就会出现了。

访问网页 ALLUXIO_MASTE:19999 来查看 Alluxio 文件系统，我们可以在结果中看到 `LICENSE` 文件，以及其他的信息。
其中 **Persstence State** 列表示文件已经被**PERSISTED**（持久化）了。

## [可选] 在 Alluxio 集群上运行一个 Hadoop MapReduce 任务

参考 [这篇文档](Running-Hadoop-MapReduce-on-Alluxio.html) 来学习如何在集群上部署 Hadoop，并运行 MapReduce。

请确保在所有节点上配置环境。

## [可选] 在 Alluxio 集群上运行 Spark 任务

参考 [这篇文档](Running-Spark-on-Alluxio.html) 来学习如何在集群上部署 Apache Spark 并执行 Spark 任务。

请确保在所有节点上配置环境。

## 关闭 Alluxio

在完成了 Alluxio 集群安装配置并进行了交互后，你可以使用以下命令来停止 Alluxio：

在 Master 节点上：

```bash
$ bin/alluxio-stop.sh master
```

在 Worker 节点上：

```bash
$ bin/alluxio-stop.sh worker
```

## 提示

如果你使用 ssh 来在所有节点上进行上述手动操作并配置，这样就可以只在 Master 节点上操作了。
配置 ssh 免密码登录，并按照[这篇文档](Running-Alluxio-on-a-Cluster.html) 操作。
凭借 `alluxio/conf/workers`, `copyDir` and `bin/alluxio-start.sh all` 命令，
你可以让如何让 Alluxio 集群更加可拓展和便捷。

## 总结

恭喜你完成了在集群上快速开始 Alluxio 的指南！你成功的下载了 Alluxio 并将它部署在了
一个 3 节点的 Linux 集群上，而且通过 Alluxio Shell 执行了一些基础的操作。
这是一个简单的 Alluxio 集群操作指南。

接下来，有很多步骤可以选择。你可以通过文档学习更多 Alluxio 的特性。你也可以在你的环境中部署 Alluxio，
挂载现有的存储系统到 Alluxio 中，并配置你的应用来使得它可以支持 Alluxio。
以下是更多的参考资源。

### 部署 Alluxio

Alluxio 可以在不同环境中部署

* [本地版本](Running-Alluxio-Locally.html)
* [集群版本](Running-Alluxio-on-a-Cluster.html)
* [在 Virtual Box 中运行](Running-Alluxio-on-Virtual-Box.html)
* [在 Docker 中运行](Running-Alluxio-On-Docker.html)
* [在 EC2 中运行](Running-Alluxio-on-EC2.html)
* [在 GCE 中运行](Running-Alluxio-on-GCE.html)
* [在 Mesos 中运行](Running-Alluxio-on-Mesos.html)
* [Alluxio 容错](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [Alluxio YARN 集成](Running-Alluxio-Yarn-Integration.html)
* [Alluxio Standalone with YARN](Running-Alluxio-Yarn-Standalone.html)

### 底层文件系统

有很多底层文件系统可以通过 Alluxio 访问。

* [Azure Blob Store](Configuring-Alluxio-with-Azure-Blob-Store.html)
* [Amazon S3](Configuring-Alluxio-with-S3.html)
* [GCS](Configuring-Alluxio-with-GCS.html)
* [Minio](Configuring-Alluxio-with-Minio.html)
* [Ceph](Configuring-Alluxio-with-Ceph.html)
* [Swift](Configuring-Alluxio-with-Swift.html)
* [GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [MapR-FS](Configuring-Alluxio-with-MapR-FS.html)
* [HDFS](Configuring-Alluxio-with-HDFS.html)
* [secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [OSS](Configuring-Alluxio-with-OSS.html)
* [NFS](Configuring-Alluxio-with-NFS.html)

### 框架和应用

Alluxio 可以支持很多框架和应用。

* [Apache Spark](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache HBase](Running-HBase-on-Alluxio.html)
* [Apache Flink](Running-Flink-on-Alluxio.html)
* [Presto](Running-Presto-with-Alluxio.html)
* [Apache Hive](Running-Hive-with-Alluxio.html)
* [Apache Zeppelin](Accessing-Alluxio-from-Zeppelin.html)
