---
layout: global
title: 概览
group: Home
priority: 0
---

* 内容列表
{:toc}

Alluxio（之前名为Tachyon）是世界上第一个以内存为中心的虚拟的分布式存储系统。它统一了数据访问的方式，为上层计算框架和底层存储系统构建了桥梁。
应用只需要连接Alluxio即可访问存储在底层任意存储系统中的数据。此外，Alluxio的以内存为中心的架构使得数据的访问速度能比现有方案快几个数量级。

在大数据生态系统中，Alluxio介于计算框架(如Apache Spark、Apache MapReduce、Apache HBase、Apache Hive、Apache Flink)和现有的存储系统（如Amazon S3、Google Cloud Storage、 OpenStack Swift、GlusterFS、HDFS、MaprFS、Ceph、NFS、OSS）之间。
Alluxio为大数据软件栈带来了显著的性能提升。例如，[百度](https://www.baidu.com)采用Alluxio使他们数据分析流水线的吞吐量提升了[30倍](http://www.alluxio.com/assets/uploads/2016/02/Baidu-Case-Study.pdf)。
巴克莱银行使用Alluxio将他们的作业分析的耗时从[小时级降到秒级](https://dzone.com/articles/Accelerate-In-Memory-Processing-with-Spark-from-Hours-to-Seconds-With-Tachyon)。
去哪儿网基于Alluxio进行[实时数据分析](http://www.alluxio.com/2016/07/qunar-performs-real-time-data-analytics-up-to-300x-faster-with-alluxio/)。
除性能外，Alluxio为新型大数据应用作用于传统存储系统的数据建立了桥梁。
用户可以以
[独立集群模式](Running-Alluxio-on-a-Cluster.html),在例如
[Amazon EC2](Running-Alluxio-on-EC2.html),
[Google Compute Engine](Running-Alluxio-on-GCE.html)运行Alluxio, 或者用
[Apache Mesos](Running-Alluxio-on-Mesos.html)或
[Apache Yarn](Running-Alluxio-Yarn-Integration.html)安装Alluxio。

Alluxio与Hadoop是兼容的。现有的数据分析应用，如Spark和MapReduce程序，可以不修改代码直接在Alluxio上运行。Alluxio是一个已在多家公司部署的开源项目([Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE))。
Alluxio是发展最快的开源大数据项目之一。自2013年4月开源以来，已有超过150个组织机构的
[700多贡献者](https://github.com/alluxio/alluxio/graphs/contributors)参与到Alluxio的开发中。包括
[阿里巴巴](http://www.alibaba.com)、 [Alluxio](http://www.alluxio.com/)、 [百度](https://www.baidu.com)、
[卡内基梅隆大学](https://www.cmu.edu/)、[Google](https://www.google.com)、[IBM](https://www.ibm.com)、[Intel](http://www.intel.com/)、
[南京大学](http://pasa-bigdata.nju.edu.cn/)、
[Red Hat](https://www.redhat.com/)、[UC Berkeley](https://amplab.cs.berkeley.edu/)和
[Yahoo](https://www.yahoo.com/)。Alluxio处于伯克利数据分析栈
([BDAS](https://amplab.cs.berkeley.edu/bdas/))的存储层，也是
[Fedora发行版](https://fedoraproject.org/wiki/SIGs/bigdata/packaging)的一部分。
到今天为止，Alluxio已经在超过100家公司的生产中进行了部署，并且在超过1000个节点的集群上运行着。

[下 载](http://alluxio.org/download/) |
[用户文档](Getting-Started.html) |
[开发者文档](Contributing-to-Alluxio.html) |
[Meetup 小组](https://www.meetup.com/Alluxio/) |
[Issue Tracking](https://github.com/Alluxio/alluxio/issues) |
[Slack Channel](https://alluxio.org/slack) |
[用户邮件列表](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[ 视频 ](https://www.youtube.com/channel/UCpibQsajhwqYPLYhke4RigA) |
[Github ](https://github.com/alluxio/alluxio/) |
[版 本](http://alluxio.org/releases/)

<style>
#current-features + ul li {height:210px;}
</style>
## 现有功能 {#current-features}
<!--for using the CSS，when tranlasting English title to Chinese,must specify the id for Chinese which is identical as the generated id in CSS for English title-->

* **[灵活的文件API](File-System-API.html)** Alluxio的本地API类似于``java.io.File``类，提供了
InputStream和OutputStream的接口和对内存映射I/O的高效支持。我们推荐使用这套API以获得Alluxio的最好性能。
另外，Alluxio提供兼容Hadoop的文件系统接口，Hadoop MapReduce和Spark可以使用Alluxio代替HDFS。

* **可插拔的底层存储** 在容错方面，Alluxio备份内存数据到底层存储系统。Alluxio提供了通用接口以简化插入
不同的底层存储系统。目前我们支持Microsoft Azure Blob Store，Amazon S3，Google Cloud Storage，OpenStack Swift，GlusterFS，
HDFS，MaprFS，Ceph，NFS，Alibaba OSS，Minio以及单节点本地文件系统，后续也会支持很多其它的文件系统。

* **[Alluxio存储](Alluxio-Storage.html)** Alluxio可以管理内存和本地存储如SSD和HDD，以加速数据访问。如果需要更细粒度的控制，分层存储功能可以用于自动管理不同层之间的数据，保证热数据在更快的存储层上。
自定义策略可以方便地加入Alluxio，而且pin的概念允许用户直接控制数据的存放位置。

* **[统一命名空间](Unified-and-Transparent-Namespace.html)** Alluxio通过挂载功能在不同的存储系统之间实
现高效的数据管理。并且，透明命名在持久化这些对象到底层存储系统时可以保留这些对象的文件名和目录层次结构。

* **[网页UI](Web-Interface.html) & [命令行](Command-Line-Interface.html)** 用户可以通过网页UI浏览文件
系统。在调试模式下，管理员可以查看每一个文件的详细信息，包括存放位置，检查点路径等等。用户也可以通
过``./bin/alluxio fs``与Alluxio交互，例如：将数据从文件系统拷入拷出。

## 快速入门

如果要快速地架设alluxio并运行，请阅读[快速入门](Getting-Started.html)页面，该页面描述了如何部署Alluxio并
在本地环境下运行几个基本的样例。
查看使用 Alluxio 的[公司名单](https://alluxio.org/community/powered-by-alluxio)。

## 下载

你可以从[Alluxio下载页面](http://alluxio.org/download)获取已发布版本。每个版本都是由已编译二进制文件组成，与各种Hadoop版本兼容。如果你想从源码编译生成Alluxio，请前往
[从Master分支构建Alluxio](Building-Alluxio-From-Source.html)。如果你有任何疑问，请通过
[用户邮箱列表](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)联系我们。 对于无法使用Google Group的用户，请访问它的[镜像](http://alluxio-users.85194.x6.nabble.com/)
(注意：该镜像可能不包含2016年五月份以前的信息)。
