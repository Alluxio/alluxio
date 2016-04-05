---
layout: global
title: 概览
group: Home
---

Alluxio（之前名为Tachyon）是世界上第一个以内存为中心的虚拟的分布式存储系统。它统一了数据访问的方式，为上层计算框架和底层存储系统构建了桥梁。
应用只需要连接Alluxio即可访问存储在底层任意存储系统中的数据。此外，Alluxio的以内存为中心的架构使得数据的访问速度能比现有常规方案快几个数量级。

在大数据生态系统中，Alluxio介于计算框架(如Apache Spark，Apache MapReduce，Apache Flink)和现有的存储系统（如Amazon S3，OpenStack Swift，GlusterFS，HDFS， Ceph，OSS）之间。
Alluxio为大数据软件栈带来了显著的性能提升。例如，[百度](https://www.baidu.com)采用Alluxio使他们数据分析流水线的吞吐量提升了[30倍](http://www.alluxio.com/assets/uploads/2016/02/Baidu-Case-Study.pdf)。
巴克莱银行使用Alluxio将他们的作业分析的耗时从[小时级降到秒级](https://dzone.com/articles/Accelerate-In-Memory-Processing-with-Spark-from-Hours-to-Seconds-With-Tachyon)。
除性能外，Alluxio为新型大数据应用作用于传统存储系统的数据建立了桥梁。
用户可以以独立集群方式(如Amazon EC2)运行Alluxio，也可以从Apache Mesos或Apache YARN上启动Alluxio。

Alluxio与Hadoop是兼容的。这意味着已有的Spark和MapReduce程序可以不修改代码直接在Alluxio上运行。Alluxio是一个已在多家公司部署的开源项目([Apache License 2.0](https://github.com/alluxio/alluxio/blob/master/LICENSE))。
Alluxio是发展最快的开源大数据项目之一。自2013年4月开源以来， 已有超过50个组织机构的
[200多贡献者](https://github.com/alluxio/alluxio/graphs/contributors)参与到Alluxio的开发中。包括
[阿里巴巴](http://www.alibaba.com), [Alluxio](http://www.alluxio.com/), [百度](https://www.baidu.com),
[卡内基梅隆大学](https://www.cmu.edu/)，[IBM](https://www.ibm.com)，[Intel](http://www.intel.com/),
[南京大学](http://pasa-bigdata.nju.edu.cn/),
[Red Hat](https://www.redhat.com/)，[UC Berkeley](https://amplab.cs.berkeley.edu/)和
[Yahoo](https://www.yahoo.com/)。Alluxio处于伯克利数据分析栈(
[BDAS](https://amplab.cs.berkeley.edu/bdas/))的存储层，也是
[Fedora发行版](https://fedoraproject.org/wiki/SIGs/bigdata/packaging)的一部分。

[Github ](https://github.com/alluxio/alluxio/) |
[ 版 本  ](http://alluxio.org/releases/) |
[ 下 载  ](http://alluxio.org/download/) |
[ 用户文档  ](Getting-Started.html) |
[ 开发者文档  ](Contributing-to-Alluxio.html) |
[ Meetup 小组  ](https://www.meetup.com/Alluxio/) |
[ JIRA  ](https://alluxio.atlassian.net/browse/ALLUXIO) |
[ 用户邮件列表  ](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users) |
[ Powered By  ](Powered-By-Alluxio.html)

<style>
#current-features + ul li {height:210px;}
</style>
# 现有功能 {#current-features}
<!--for using the CSS，when tranlasting English title to Chinese,must specify the id for Chinese which is identical as the generated id in CSS for English title-->

* **[灵活的文件API](File-System-API.html)** Alluxio的本地API类似于``java.io.File``类，提供了
InputStream和OutputStream的接口和对内存映射I/O的高效支持。我们推荐使用这套API以获得Alluxio的最好性能。
另外，Alluxio提供兼容Hadoop的文件系统接口，Hadoop MapReduce和Spark可以使用Alluxio代替HDFS。

* **可插拔的底层存储** 在容错方面，Alluxio备份内存数据到底层存储系统。Alluxio提供了通用接口以简化插入
不同的底层存储系统。目前我们支持Amazon S3，OpenStack Swift，Apache HDFS，GlusterFS以及单节点本地文件系
统，后续也会支持很多其它的文件系统。

* **[层次化存储](Tiered-Storage-on-Alluxio.html)** 通过分层存储，Alluxio不仅可以管理内存，也可以管理SSD
和HDD,能够让更大的数据集存储在Alluxio上。数据在不同层之间自动被管理，保证热数据在更快的存储层上。自定义策
略可以方便地加入Alluxio，而且pin的概念允许用户直接控制数据的存放位置。

* **[统一命名空间](Unified-and-Transparent-Namespace.html)** Alluxio通过挂载功能在不同的存储系统之间实
现高效的数据管理。并且，透明命名在持久化这些对象到底层存储系统时可以保留这些对象的文件名和目录层次结构。

* **[世系(Lineage)](Lineage-API.html)** 通过世系(Lineage)，Alluxio可以不受容错的限制实现高吞吐的写入，
丢失的输出可以通过重新执行创建这一输出的任务来恢复。应用将输出写入内存，Alluxio以异步方式定期备份数据到底层
文件系统。写入失败时，Alluxio启动任务重执行恢复丢失的文件。

* **[网页UI](Web-Interface.html) & [命令行](Command-Line-Interface.html)** 用户可以通过网页UI浏览文件
系统。在调试模式下，管理员可以查看每一个文件的详细信息，包括存放位置，检查点路径等等。用户也可以通
过``./bin/alluxio fs``与Alluxio交互，例如：将数据从文件系统拷入拷出。

# 快速入门

如果要快速地架设alluxio并运行，请阅读[快速入门](Getting-Started.html)页面，该页面描述了如何部署Alluxio并
在本地环境下运行几个基本的样例。

# 下载

你可以从[Alluxio下载页面](http://alluxio.org/download)获取已发布版本。每个版本都是由已编译二进
制文件组成，与各种Hadoop版本兼容。如果你想从源码编译生成Alluxio，请前往
[从Master分支构建Alluxio](Building-Alluxio-Master-Branch.html)。
