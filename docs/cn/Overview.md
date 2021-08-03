---
layout: global
title: Alluxio概览
group: Overview
priority: 0
---

* 内容列表
{:toc}
## 什么是 Alluxio
Alluxio 是世界上第一个面向基于云的数据分析和人工智能的开源的[数据编排技术](https://www.alluxio.io/blog/data-orchestration-the-missing-piece-in-the-data-world/)。
它为数据驱动型应用和存储系统构建了桥梁, 将数据从存储层移动到距离数据驱动型应用更近的位置从而能够更容易被访问。
这还使得应用程序能够通过一个公共接口连接到许多存储系统。
Alluxio内存至上的层次化架构使得数据的访问速度能比现有方案快几个数量级。

在大数据生态系统中，Alluxio 位于数据驱动框架或应用（如 Apache Spark、Presto、Tensorflow、Apache HBase、Apache Hive 或 Apache Flink）和各种持久化存储系统（如 Amazon S3、Google Cloud Storage、OpenStack Swift、HDFS、GlusterFS、IBM Cleversafe、EMC ECS、Ceph、NFS 、Minio和 Alibaba OSS）之间。
Alluxio 统一了存储在这些不同存储系统中的数据，为其上层数据驱动型应用提供统一的客户端 API 和全局命名空间。

Alluxio 项目源自 UC Berkeley 的 [AMPLab](https://amplab.cs.berkeley.edu/software/)（见[论文](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html)），在伯克利数据分析栈 (Berkeley Data Analytics Stack, [BDAS](https://amplab.cs.berkeley.edu/bdas/)) 中扮演数据访问层的角色。
它以 Apache License 2.0 协议的方式开源。
Alluxio 是发展最快的开源大数据项目之一，已经吸引了超过 300 个组织机构的[1000多名贡献者](https://github.com/alluxio/alluxio/graphs/contributors)参与到 Alluxio 的开发中，包括
[阿里巴巴](http://www.alibaba.com)、
[Alluxio](https://www.alluxio.io)、
[百度](https://www.baidu.com)、
[CMU](https://www.cmu.edu/)、
[Google](https://www.google.com)、
[IBM](https://www.ibm.com)、
[Intel](http://www.intel.com/)、
[南京大学](http://www.nju.edu.cn/english/)、
[Red Hat](https://www.redhat.com/)、
[腾讯](https://www.tencent.com)、
[UC Berkeley](https://amplab.cs.berkeley.edu/)、
和 [Yahoo](https://www.yahoo.com/)。

到今天为止，Alluxio 已经在[数百家机构](https://www.alluxio.io/powered-by-alluxio)的生产中进行了部署，最大部署运行的集群规模超过 1500 个节点。

<p align="center">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2021/07/alluxio-overview-r071521.png" width="800" alt="Ecosystem"/>
</p>

## 优势

通过简化应用程序访问其数据的方式（无论数据是什么格式或位置），Alluxio 能够帮助克服从数据中提取信息所面临的困难。Alluxio 的优势包括：

* **内存速度 I/O**：Alluxio 能够用作分布式共享缓存服务，这样与 Alluxio 通信的计算应用程序可以透明地缓存频繁访问的数据（尤其是从远程位置），以提供内存级 I/O 吞吐率。此外，Alluxio的层次化存储机制能够充分利用内存、固态硬盘或者磁盘，降低具有弹性扩张特性的数据驱动型应用的成本开销。

* **简化云存储和对象存储接入**：与传统文件系统相比，云存储系统和对象存储系统使用不同的语义，这些语义对性能的影响也不同于传统文件系统。在云存储和对象存储系统上进行常见的文件系统操作（如列出目录和重命名）通常会导致显著的性能开销。当访问云存储中的数据时，应用程序没有节点级数据本地性或跨应用程序缓存。将 Alluxio 与云存储或对象存储一起部署可以缓解这些问题，因为这样将从 Alluxio 中检索读取数据，而不是从底层云存储或对象存储中检索读取。

* **简化数据管理**：Alluxio 提供对多数据源的单点访问。除了连接不同类型的数据源之外，Alluxio 还允许用户同时连接同一存储系统的不同版本，如多个版本的 HDFS，并且无需复杂的系统配置和管理。

* **应用程序部署简易**：Alluxio 管理应用程序和文件或对象存储之间的通信，将应用程序的数据访问请求转换为底层存储接口的请求。Alluxio 与 Hadoop 生态系统兼容，现有的数据分析应用程序，如 Spark 和 MapReduce 程序，无需更改任何代码就能在 Alluxio 上运行。

## 技术创新

Alluxio 将三个关键领域的创新结合在一起，提供了一套独特的功能。

1. **全局命名空间**：Alluxio 能够对多个独立存储系统提供单点访问，无论这些存储系统的物理位置在何处。这提供了所有数据源的统一视图和应用程序的标准接口。有关详细信息，请参阅[统一命名空间文档]({{ '/cn/core-services/Unified-Namespace.html' | relativize_url }})。
1. **智能多层级缓存**：Alluxio 集群能够充当底层存储系统中数据的读写缓存。可配置自动优化数据放置策略，以实现跨内存和磁盘（SSD/HDD）的性能和可靠性。缓存对用户是透明的，使用缓冲来保持与持久存储的一致性。有关详细信息，请参阅 [缓存功能文档]({{ '/cn/core-services/Caching.html' | relativize_url }})。
1. **服务器端 API 翻译转换**：Alluxio支持工业界场景的API接口，例如HDFS API, S3 API, FUSE API, REST API。它能够透明地从标准客户端接口转换到任何存储接口。Alluxio 负责管理应用程序和文件或对象存储之间的通信，从而消除了对复杂系统进行配置和管理的需求。文件数据可以看起来像对象数据，反之亦然。

## 快速上手指南

如果打算快速地搭建 Alluxio 并运行，请阅读[快速上手指南]({{ '/cn/overview/Getting-Started.html' | relativize_url }})页面，该页面描述了如何部署 Alluxio 并在本地环境下运行示例。

或者，你也可以尝试我们为Presto & Alluxio制作好的快速上手教程，具体点击如下图片链接:
<p align="center">
<a href="https://www.alluxio.io/alluxio-presto-sandbox-docker/">
 <img src="https://www.alluxio.io/app/uploads/2019/07/laptop-docker.png" width="250" alt="Laptop with Docker"/></a>
</p>

## 下载和有用资源

你可以从 [Alluxio 下载页面](https://alluxio.io/download)获取已发布版本。
每个Alluxio发布版本都提供了与不同 Hadoop 版本兼容的预编译好的二进制文件。
[从 Master 分支构建 Alluxio页面]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }})解释了如何从源代码编译生成Alluxio项目。
如果你有任何疑问，请联系我们[用户邮件列表](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)（
对于无法使用 Google Group 的用户，请使用它的[镜像](http://alluxio-users.85194.x6.nabble.com/))
或者我们的[社区Slack频道](https://alluxio.io/slack).。

Alluxio微信公众号二维码（下图）
<p align="left">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2019/04/public_account_qrcode.png" width="120" alt="QRCode"/>
</p>

[下载](https://alluxio.io/download/) |
[用户文档]({{ '/cn/overview/Getting-Started.html' | relativize_url }}) |
[开发者文档]({{ '/cn/contributor/Contributor-Getting-Started.html' | relativize_url }}) |
[Meetup](https://www.meetup.com/Alluxio/) |
[Issue Tracking](https://github.com/Alluxio/alluxio/issues) |
[社区Slack频道](https://alluxio.io/slack) |
[用户邮件列表](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[视频](https://www.youtube.com/channel/UCpibQsajhwqYPLYhke4RigA) |
[Github](https://github.com/alluxio/alluxio/) |
[已发布版本](https://www.alluxio.io/download/releases)
