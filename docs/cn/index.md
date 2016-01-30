---
layout: global
title: 概览
group: Home
---

Tachyon是一个以内存为核心的开源分布式存储系统，能够为不同计算框架（如Apache Spark，Apache MapReduce，Apache
Flink）下编写的集群任务提供可靠的内存级数据共享。在大数据生态系统中，Tachyon介于计算框架(如Apache Spark，Apache MapReduce，Apache Flink)和现有的存储系统（如Amazon S3，OpenStack Swift，GlusterFS，HDFS，Ceph，OSS）之间。Tachyon为大数据软件栈带来了显著的性能提升。以[百度](https://www.baidu.com)为例，使用Tachyon后，其数据处理性能提升了30倍。除性能外，Tachyon为新型大数据应用作用于传统存储系统的数据建立了桥梁。用户可以以独立集群方式(如Amazon EC2)运行Tachyon,或者从Apache Mesos或Apache Yarn上启动Tachyon。

Tachyon与Hadoop兼容。这意味着已有的Spark和MapReduce程序可以不修改代码直接在Tachyon上运行。Tachyon是开源项目
([Apache License 2.0](https://github.com/amplab/tachyon/blob/master/LICENSE))，已在多家公司部署。Tachyon是发展最快的开源大数据项目之一。自2013年4月开源以来， 已有超过50个组织机构的
[200多贡献者](https://github.com/amplab/tachyon/graphs/contributors)参与到Tachyon的开发中。包括 [阿里巴巴](http://www.alibaba.com), [百度](https://www.baidu.com), [CMU](https://www.cmu.edu/)，[IBM](https://www.ibm.com)， [Intel](http://www.intel.com/),[ Red Hat](https://www.redhat.com/)，[Tachyon Nexus](http://www.tachyonnexus.com/), [UC Berkeley](https://amplab.cs.berkeley.edu/)和[Yahoo](https://www.yahoo.com/)。Tachyon处于伯克利数据分析栈
([BDAS](https://amplab.cs.berkeley.edu/bdas/))的存储层，也是[Fedora发行版](https://fedoraproject.org/wiki/SIGs/bigdata/packaging)的一部分

[Github ](https://github.com/amplab/tachyon/) |
[ 版 本  ](http://tachyon-project.org/releases/) |
[ 下 载  ](http://tachyon-project.org/downloads/) |
[ 用户文档  ](Getting-Started.html) |
[ 开发者文档  ](Contributing-to-Tachyon.html) |
[ Meetup 小组  ](https://www.meetup.com/Tachyon/) |
[ JIRA  ](https://tachyon.atlassian.net/browse/TACHYON) |
[ 用户邮件列表  ](https://groups.google.com/forum/?fromgroups#!forum/tachyon-users) |
[ Powered By  ](Powered-By-Tachyon.html)

<style>
#current-features + ul li {height:210px;}
</style>
# 现有功能 {#current-features}
<!--for using the CSS，when tranlasting English title to Chinese,must specify the id for Chinese which is identical as the generated id in CSS for English title-->

* **[灵活的文件API](File-System-API.html)** Tachyon的本地API类似于``java.io.File``类，提供了InputStream和OutputStream的接口和对内存映射I/O的高效支持。我们推荐使用这套API以获得Tachyon的最好性能。另外，Tachyon提供兼容Hadoop的文件系统接口，Hadoop MapReduce和Spark可以使用Tachyon代替HDFS。

* **可插拔的底层存储** 在容错方面，Tachyon定期备份内存数据到底层存储系统。Tachyon提供了通用接口以简化插入不同的底层存储系统。目前我们支持Amazon S3，OpenStack Swift，Apache HDFS，GlusterFS以及单节点本地文件系统，后续也会支持很多其它的文件系统。

* **[层次化存储](Tiered-Storage-on-Tachyon.html)** 通过分层存储，Tachyon不仅可以管理内存，也可以管理SSD和HDD,能够让更大的数据集存储在Tachyon上。数据在不同层之间自动被管理，保证热数据在更快的存储层上。自定义策略可以方便地加入Tachyon，而且pin的概念允许用户直接控制数据的存放位置。

* **[统一命名空间](Unified-and-Transparent-Namespace.html)** Tachyon通过挂载功能在不同的存储系统之间实现高效的数据管理。并且，透明命名在持久化这些对象到底层存储系统时可以保留这些对象的文件名和目录层次结构

* **[血统(Lineage)](Lineage-API.html)** 通过血统(Lineage)，Tachyon可以不受容错的限制实现高吞吐的写入，丢失的输出可以通过重新执行创建这一输出的任务来恢复。应用将输出写入内存，Tachyon以异步方式定期备份数据到底层文件系统。写入失败时，Tachyon启动任务重执行恢复丢失的文件。

* **[网页UI](Web-Interface.html) & [命令行](Command-Line-Interface.html)** 用户可以通过网页UI浏览文件系统。在调试模式下，管理员可以查看每一个文件的详细信息，包括存放位置，检查点路径等等。用户也可以通过``./bin/tachyon tfs``与Tachyon交互，例如：将数据从文件系统拷入拷出。

# 快速入门

为了快速地启动tachyon并运行，阅读一下[快速入门](Getting-Started.html)页面，该页面描述了如何部署Tachyon并在本地环境下运行几个基本的样例。

# 下载

你可以从[Tachyon下载页面](http://tachyon-project.org/downloads)获取已发布版本。每个版本都是由已编译二进制文件组成，与各种Hadoop版本兼容。如果你想从源码编译生成Tachyon，请前往[从Master分支构建Tachyon](Building-Tachyon-Master-Branch.html).
