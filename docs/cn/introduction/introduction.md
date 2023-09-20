---
layout: global
title: Introduction
---

请查看以下两个短视频，了解 Alluxio 旨在解决的现有数据问题以及 Alluxio 在大数据生态系统中的地位
* ▶️ [需要解决的问题](https://www.youtube.com/watch?v=_zenG90idAA){:target="_blank"} (3:06)
* ▶️ [什么是 Alluxio？](https://www.youtube.com/watch?v=py-kfEGRDZA){:target="_blank"} (2:50)

## 关于Alluxio

Alluxio 是全球首个用于云中分析和人工智能的开源[数据编排技术](https://www.alluxio.io/blog/data-orchestration-the-missing-piece-in-the-data-world/)。Alluxio 架起了数据驱动应用程序与存储系统之间的鸿沟，将数据从存储层带到数据驱动应用程序更近的位置，使其易于访问，从而使应用程序能够通过一个通用接口连接到众多存储系统。Alluxio 的内存优先分层架构使数据访问速度比现有解决方案快了数个数量级。

在数据生态系统中，Alluxio 位于数据驱动应用程序（如Apache Spark、Presto、Tensorflow、Apache HBase、Apache Hive或Apache Flink）与各种持久性存储系统（如Amazon S3、Google Cloud Storage、HDFS、IBM Cleversafe、EMC ECS、Ceph、NFS、Minio和Alibaba OSS）之间。Alluxio 统一了存储在这些不同存储系统中的数据，为其上层的数据驱动应用程序提供了统一的客户端API和全局命名空间。

Alluxio 项目起源于[加州大学伯克利分校的AMPLab](https://amplab.cs.berkeley.edu/software/)(请参阅相关[论文](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html))，作为伯克利数据分析堆栈([BDAS](https://amplab.cs.berkeley.edu/bdas/))的数据访问层。
它是在[Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE)下的开源项目。
Alluxio 是增长最快的开源项目之一，吸引了来自300多家机构的[1000多名贡献者](https://github.com/alluxio/alluxio/graphs/contributors)，包括阿里巴巴、百度、卡内基梅隆大学（CMU）、谷歌、IBM、英特尔、南京大学（NJU）、红帽、腾讯、加州大学伯克利分校（UC Berkeley）和雅虎。

如今，Alluxio 已经被数百个组织部署在生产环境中，其中最大的部署超过了1500个节点。

<p align="center">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2021/07/alluxio-overview-r071521.png" width="800" alt="Ecosystem"/>
</p>

## Dora架构

"Dora"，全称为"Decentralized Object Repository Architecture"，是Alluxio系统的基础。

作为一个开源的分布式缓存存储系统，Dora提供低延迟、高吞吐量和成本节省，同时旨在提供一个统一的数据层，支持各种数据工作负载，包括人工智能和数据分析。

Dora利用分散式存储和元数据管理，以提供更高的性能和可用性，以及可插拔的数据安全和治理，实现更好的可扩展性和更有效的大规模数据访问管理。

Dora的架构目标是：
* 可扩展性：可扩展性是Dora的首要任务，它需要支持数十亿个文件，以满足数据密集型应用（如AI训练）的需求。
* 高可用性：Dora的架构旨在具备高可用性，拥有99.99%的正常运行时间并保护免受单点故障的影响。
* 性能：性能是Dora的关键目标，它优先考虑Presto/Trino支持的SQL分析工作负载和用于AI工作负载的GPU利用率。

下面的图表展示了Dora的架构设计，包括四个主要组件：服务注册表、调度器、客户端和工作节点。

![Dora Architecture]({{ '/img/dora_architecture.png' | relativize_url }})

* 工作节点是最重要的组件，因为它存储着按关键字划分的元数据和数据，通常使用文件路径作为关键字。
* 客户端运行在应用程序内部，利用相同的一致性哈希算法来确定相应文件的适当工作节点。
* 服务注册表负责服务发现并维护工作节点列表。
* 调度器处理所有异步作业，例如将数据预加载到工作节点。

## Alluxio 社区 Slack

加入我们充满活力且快速增长的[Alluxio社区Slack频道](https://www.alluxio.io/slack)，与Alluxio的用户和开发者建立联系。如果您需要帮助运行Alluxio或遇到任何问题，请将您的技术问题发送到我们的`#troubleshooting`频道。如果您是一名希望为Alluxio做贡献的开发者，请查看`#dev`频道。