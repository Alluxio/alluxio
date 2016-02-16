---
layout: global
title: 结构概述
nickname: 结构概述
group: User Guide
priority: 1
---

* Table of Contents
{:toc}

# Tachyon的应用场景

由于Tachyon以内存为中心，并且是数据访问的核心，所以Tachyon在大数据生态圈里占有独特地位，它居于传统大数据存储（如：Amazon S3，Apache HDFS和OpenStack Swift等）和大数据计算框架（如Spark，Hadoop Mapreduce）之间。对于用户应用和计算框架，无论其是否运行在相同的计算引擎之上，Tachyon都可以作为底层来支持数据的访问、快速存储，以及多任务的数据共享和本地化。因此，Tachyon可以为大数据应用带来一个数量级的加速，同时它还提供了通用的数据访问接口。对于底层存储系统，Tachyon连接了大数据应用和传统存储系统之间的缝隙，并且重新定义了一组面向数据使用的大数据分析应用。因Tachyon对应用屏蔽了底层存储系统的整合过程，所以任何底层存储系统都可以支撑运行在Tachyon之上的应用和框架。此外Tachyon可以挂载多种底层存储系统，所以它可以作为统一层为任意数量的不同数据源提供服务。

![Stack]({{site.data.img.stack}})

# Tachyon的组件

Tachyon的设计使用了单Master和多Worker的模式。在最上层，Tachyon可以被分为三个部分，[Master](#Master)，[Worker](#Worker)和[Client](#Client)。Master和Worker一起组成了Tachyon的服务端，它们是系统管理员维护和管理的组件。Client通常是应用程序，如Spark或MapReduce作业，或者Tachyon的命令行用户。Tachyon用户一般只与Tachyon的Client进行交互。

### Master

Tachyon有两种Master部署模式，可以用其中一种进行部署, [单Master模式](Running-Tachyon-Locally.html)或[多Master模式（一个主要的加一个备用的）](Running-Tachyon-Fault-Tolerant-on-EC2.html)。Master主要负责处理全局的系统元数据，例如，文件系统树。Client可以通过与Master的交互来读取或修改元数据。此外所有的Worker会周期性地发送心跳给Master，来确保它们在集群中。Master不会发起与其他组件的通信，它只是与其他组件以回复请求的方式进行通信。

### Worker

Tachyon的Worker负责处理分配给Tachyon的[本地资源](Tiered-Storage-on-Tachyon.html)。这些资源可以是本地内存，SDD或者硬盘，其可以由用户配置。Tachyon的Worker以块的形式存储数据，并通过读或创建数据块的方式处理来自Client读写数据的请求。但Worker只负责这些数据块上的数据；文件到块的实际映射只会存储在Master上。

### Client

Tachyon的Client为用户提供了一个与Tachyon服务端交互的入口。它为用户暴露了一组[文件系统API](File-System-API.html)。Client通过发起与Master的通信来执行元数据操作，并且通过与Worker通信来读取Tachyon上的数据或者向Tachyon上写数据。底层存储系统上、但不是Tachyon上的数据可以直接通过底层存储客户端访问。
