---
layout: global
title: 在Alluxio上运行Apache Flink
nickname: Apache Flink
group: Frameworks
priority: 2
---

该指南介绍如何在Alluxio上运行[Apache Flink](http://flink.apache.org/),以便你在Flink中使用Alluxio的文件。

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)。同时使用[本地模式](Running-Alluxio-Locally.html)或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

请在[Apache Flink](http://flink.apache.org/)网站上阅读Flink安装说明。

# 配置

Apache Flink可以通过通用文件系统包装类（可用于Hadoop文件系统）来使用Alluxio。因此，Alluxio的配置主要在Hadoop配置文件中完成。


#### 在`core-site.xml`中设置属性

如果你安装Flink的同时安装了Hadoop，将如下属性加到`core-site.xml`配置文件：

{% include Running-Flink-on-Alluxio/core-site-configuration.md %}

如果你没有安装Hadoop，创建一个包含以下内容的`core-site.xml`文件

{% include Running-Flink-on-Alluxio/create-core-site.md %}

#### 在`conf/flink-config.yaml`中指定`core-site.xml`的路径

接下来需要指定Flink中Hadoop配置的路径。打开Flink根目录下`conf/flink-config.yaml`文件，设置`fs.hdfs.hadoopconf`的值为`core-site.xml`的**目录**（对于新的Hadoop版本，该目录通常以`etc/hadoop`结尾）。 

#### 构建及布置Alluxio客户端Jar包

为了与Alluxio通信，需要提供带有Alluxio核心客户端Jar包的Flink程序。要构建与Flink兼容的客户端Jar包，需在Alluxio工程根目录下指定Flink选项构建整个工程：

{% include Running-Flink-on-Alluxio/flink-profile-build.md %}

接下来需要让Alluxio `jar`文件对Flink可用，因为其中包含了配置好的`alluxio.hadoop.FileSystem`类。

有以下几种方式实现：

- 将`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件放在Flink的`lib`目录下（对于本地模式以及独立集群模式）。
- 将`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件放在布置在Yarn中的Flink下的`ship`目录下。
- 在`HADOOP_CLASSPATH`环境变量中指定该jar文件的路径（要保证该路径对集群中的所有节点都有效）。例如：

{% include Running-Flink-on-Alluxio/hadoop-classpath.md %}

# 在Flink中使用Alluxio

Flink中使用Alluxio，指定路径时使用`alluxio://`前缀。

如果Alluxio是本地安装，有效路径类似于：
`alluxio://localhost:19998/user/hduser/gutenberg`。

## Wordcount示例

该示例假定你已经按前文指导安装了Alluxio和Flink。

将`LICENSE`文件放入Alluxio中，假定当前目录为Alluxio工程的根目录：

{% include Running-Flink-on-Alluxio/license.md %}

在Flink工程的根目录下运行以下命令：

{% include Running-Flink-on-Alluxio/wordcount.md %}

接着打开浏览器，进入[http://localhost:19999/browse](http://localhost:19999/browse)，其中应存在一个`output`文件，该文件即为对`LICENSE`文件进行word count的结果。
