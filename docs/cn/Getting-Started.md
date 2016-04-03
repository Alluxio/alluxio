---
layout: global
title: 开始
group: User Guide
priority: 0
---

* Table of Contents
{:toc}

### 安装Alluxio

Alluxio可以以多种配置模式进行安装。对于新用户，最简单的安装方式就是[本地运行Alluxio](Running-Alluxio-Locally.html)。要想用集群安装来进行实验，可以参考[virtual box](Running-Alluxio-on-Virtual-Box.html)或[Amazon AWS](Running-Alluxio-on-EC2.html)的教程。

### 配置底层存储

Alluxio可以看作一个数据交换层，并且得益于有可靠的持久存储的支撑。不同底层存储的选择依赖于具体的生产环境。通过提供的底层存储连接器，Alluxio可以与任何底层存储整合。现在[Amazon S3](Configuring-Alluxio-with-S3.html)，[OpenStack Swift](Configuring-Alluxio-with-Swift.html)，[GlusterFS](Configuring-Alluxio-with-GlusterFS.html)，和[Apache HDFS](Configuring-Alluxio-with-HDFS.html)都已支持Alluxio。

### 配置应用程序

Alluxio为应用程序提供了一个[文件系统接口](File-System-API.html)，从而使其可以操作存储在Alluxio里的数据。如果你想在Alluxio之上直接写应用程序，只需给你的项目添加`alluxio-core-client`依赖。例如，如果是用Maven构建的应用程序：

{% include Getting-Started/config-application.md %}

有些利用Alluxio的特定应用是计算框架。很容易就可以将这些框架改为使用Alluxio，尤其是已经与Hadoop文件系统接口整合了的计算框架。因为Alluxio也提供了该接口的实现，唯一需要修改的就是将数据路径从`hdfs://master-hostname:port`变为`alluxio://master-hostname:port`。参考[Apache Spark](Running-Spark-on-Alluxio.html)，[Apache Hadoop MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html)或[Apache Flink](Running-Flink-on-Alluxio.html)的教程。

### 配置系统

Alluxio有各种调节系统的参数，从而使其在不同的使用环境下发挥最好的性能。对于一个应用程序，Alluxio从特定的`alluxio-site.properties`文件或命令行传递的Java选项来读取自定义配置。对于具体的可调参数，详情参见[配置设置](Configuration-Settings.html)。

### 其他功能

Alluxio除了提供数据存储快速共享层，还有很多对开发者和管理员有用的特性。

* [命令行接口](Command-Line-Interface.html)允许用户通过轻量级的shell命令来访问和操作Alluxio上的数据。
* [度量系统](Metrics-System.html)使用户方便地去监视系统的状态，并且发现应用程序效率低的瓶颈。
* [Web接口](Web-Interface.html)对Alluxio里的数据提供了直观丰富的表示，但目前只支持只读视图。

### 高级功能

Alluxio除了通过加速数据的输入输出来显著地提升性能之外，还提供了如下的高级功能。

* [层级存储](Tiered-Storage-on-Alluxio.html)为Alluxio提供了额外的管理资源（如SDD或HDD），其允许无法存入内存的数据依然可以利用Alluxio架构的优势。
* [统一而透明的命名空间](Unified-and-Transparent-Namespace.html)为用户提供了综合管理现有存储系统数据的能力。即使已部署的存储系统不能感知到Alluxio，用户仍然可以通过Alluxio很容易地对这些已部署的存储系统里的数据统一操作。
* [世系关系（Lineage）](Lineage-API.html)，相对于开销很大的磁盘备份来说，其提供了一个用于容错和数据持久化的新方案，可以极大地提升写性能。

