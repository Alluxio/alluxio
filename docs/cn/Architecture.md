---
layout: global
title: 架构概述
nickname: 架构概述
group: User Guide
priority: 1
---

* 内容列表
{:toc}

## Alluxio的应用场景

由于Alluxio的设计以内存为中心，并且是数据访问的中心，所以Alluxio在大数据生态圈里占有独特地位，它居于大数据存储（如：Amazon S3，Apache HDFS和OpenStack 
Swift等和大数据计算框架（如Spark，Hadoop Mapreduce）之间。对于用户应用和计算框架，无论其是否运行在相同的计算引擎之上，Alluxio都可以作为底层来支持数据的
访问、快速存储，以及多任务的数据共享和本地化。因此，Alluxio可以为那些大数据应用提供一个数量级的加速，同时它还提供了通用的数据访问接口。对于底层存储系统，
Alluxio连接了大数据应用和传统存储系统之间的间隔，并且重新定义了一组面向数据使用的工作负载程序。因Alluxio对应用屏蔽了底层存储系统的整合细节，所以任何底层
存储系统都可以支撑运行在Alluxio之上的应用和框架。此外Alluxio可以挂载多种底层存储系统，所以它可以作为统一层为任意数量的不同数据源提供服务。

![Stack]({{site.data.img.stack}})

## Alluxio的组件

Alluxio的设计使用了单Master和多Worker的架构。从高层的概念理解，Alluxio可以被分为三个部分，[Master](#Master)，[Worker](#Worker)和[Client](#Client)。
Master和Worker一起组成了Alluxio的服务端，它们是系统管理员维护和管理的组件。Client通常是应用程序，如Spark或MapReduce作业，或者Alluxio的命令行用户。
Alluxio用户一般只与Alluxio的Client组件进行交互。

#### Master
Alluxio Master有主从两种模式。

##### 主Master
主Master主要负责处理全局的系统元数据，例如，文件系统树。Client可以通过与Master的交互来读取或修改元数据。此外所有的Worker会周期性地发送心跳给主Master，
来确保它们还参与在Alluxio集群中。主Master不会主动发起与其他组件的通信，它只是以回复请求的方式与其他组件进行通信。一个Alluxio集群只有一个主Master。

##### 从Master
从Master不断的读取并处理主Master写的日志。同时从Master会周期性的把所有的状态写入日志。从Master不处理任何请求。

##### Master部署
Alluxio Master有[简单](#Running-Alluxio-Locally.html)和[高可用性](#Running-Alluxio-Fault-Tolerant.html)
两种部署模式。这两种模式都只有一个主Master。简单模式最多只会有一个从Master，而且这个从Master不会被转换为主Maste。高可用性模式可以有零个或者多个从Master。
当主Master异常的时候，系统会选一个从Master担任新的主Master。

#### Worker

Alluxio的Worker负责管理分配给Alluxio的[本地资源](Tiered-Storage-on-Alluxio.html)。这些资源可以是本地内存，SDD或者硬盘，其可以由用户配置。
Alluxio的Worker以块的形式存储数据，并通过读或创建数据块的方式处理来自Client读写数据的请求。但Worker只负责这些数据块上的数据；文件到块的实际映
射只会存储在Master上。

#### Client

Alluxio的Client为用户提供了一个与Alluxio服务端交互的入口。它为用户暴露了一组[文件系统API](File-System-API.html)。Client通过发起与Master
的通信来执行元数据操作，并且通过与Worker通信来读取Alluxio上的数据或者向Alluxio上写数据。存储在底层存储系统上而不是Alluxio上的数据可以直接通过
底层存储客户端访问。
