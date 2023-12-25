---
layout: global
title: Introduction
---

请查看以下两个短视频，了解 Alluxio 旨在解决的现有数据问题，以及 Alluxio 在大数据生态系统中的位置。
* ▶️ [需要解决的问题](https://www.youtube.com/watch?v=_zenG90idAA){:target="_blank"} (3:06)
* ▶️ [什么是 Alluxio？](https://www.youtube.com/watch?v=py-kfEGRDZA){:target="_blank"} (2:50)

## 关于Alluxio

Alluxio 是全球首个用于云上分析和人工智能的开源[数据编排技术](https://www.alluxio.io/blog/data-orchestration-the-missing-piece-in-the-data-world/)。Alluxio 架起了连接了数据驱动应用与存储系统，将数据从存储带到数据驱动应用更近的位置，使数据易于访问，并允许应用通过一个通用接口连接到众多存储系统。Alluxio 的内存优先的分级架构使得数据访问速度比现有其他解决方案快了几个数量级。

在数据生态系统中，Alluxio 位于数据驱动应用（如Apache Spark、Presto、Tensorflow、Apache HBase、Apache Hive或Apache Flink）与各种持久性存储系统（如Amazon S3、Google Cloud Storage、HDFS、IBM Cleversafe、EMC ECS、Ceph、NFS、Minio和Alibaba OSS）之间。Alluxio 统一位于不同存储系统中的数据，为其上层的数据驱动应用提供了统一的客户端API和全局命名空间。

Alluxio 项目起源于[加州大学伯克利分校的AMPLab](https://amplab.cs.berkeley.edu/software/)(请参阅相关[论文](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2018/EECS-2018-29.html))，作为伯克利数据分析堆栈([BDAS](https://amplab.cs.berkeley.edu/bdas/))的数据访问层。
它是在[Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE)下的开源项目。
Alluxio 是增长最快的开源项目之一，吸引了来自300多家机构的[1000多名贡献者](https://github.com/alluxio/alluxio/graphs/contributors)，机构包括阿里巴巴、百度、卡内基梅隆大学（CMU）、谷歌、IBM、英特尔、南京大学（NJU）、红帽、腾讯、加州大学伯克利分校（UC Berkeley）和雅虎。

如今，数百个组织已在其生产环境中部署了Alluxio，其中最大的部署规模超过了1500个节点。

<p align="center">
<img src="https://d39kqat1wpn1o5.cloudfront.net/app/uploads/2021/07/alluxio-overview-r071521.png" width="800" alt="Ecosystem"/>
</p>

## Dora架构

"Dora"，全称为"Decentralized Object Repository Architecture(去中心化对象储存库架构)"，是Alluxio系统的基础。

作为一个开源的分布式缓存存储系统，Dora能实现低延迟、高吞吐且节约成本，同时提供统一的数据层，支持各种数据工作负载，包括人工智能和数据分析。

Dora利用去中心化的存储和元数据管理来提供更高的性能和数据可用性，以及可插拔的数据安全和治理，实现更好的可扩展性和更有效的大规模数据访问管理。

Dora的架构目标是：
* 可扩展性：可扩展性是Dora的首要任务，它需要能够支持数十亿个文件，满足数据密集型应用(如AI训练)的需求。
* 高可用性：Dora的架构旨在确保高可用性，拥有99.99%的正常运行时间并提供单点故障保护。
* 性能：性能是Dora的关键目标，它优先考虑Presto/Trino支持的SQL分析工作负载和AI工作负载的GPU利用率。

下面的图表展示了Dora的架构设计，包括四个主要组件：服务注册表(service registry)、调度器(scheduler)、客户端(client)和worker节点。

![Dora Architecture]({{ '/img/dora_architecture.png' | relativize_url }})

* worker节点是最重要的组件，因为它存储了按键(通常是文件路径)分片的元数据和数据。
* 客户端运行在应用程序内部，利用相同的一致性哈希算法来确定相应文件的对应worker节点。
* 服务注册表负责服务发现并维护worker节点列表。
* 调度器处理所有异步作业，例如将数据预加载到worker节点。

## Alluxio 社区 Slack

[Alluxio社区Slack频道](https://www.alluxio.io/slack)气氛活跃，人数也在快速增长。欢迎加入Alluxio社区Slack，与我们的用户和开发者沟通互动。如果您在运行Alluxio的过程中遇到任何问题或需要任何帮助，请将您的技术问题发送到我们的`#troubleshooting`频道。如果您是一名开发者，希望为Alluxio做贡献，请查看`#dev`频道。