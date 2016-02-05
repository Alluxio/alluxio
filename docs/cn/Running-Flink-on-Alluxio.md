---
layout: global
title: 在Alluxio上运行Apache Flink
nickname: Apache Flink
group: Frameworks
priority: 2
---

该向导介绍如何在Alluxio上运行[Apache Flink](http://flink.apache.org/),以便你在Flink中使用Alluxio的文件

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)。同时使用[本地模式](Running-Alluxio-Locally.html)或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

请在[Apache Flink](http://flink.apache.org/)网站上阅读Flink安装说明。

# 配置

Apache Flink可以通过通用文件系统包装类（可用于Hadoop文件系统）来使用Alluxio。因此，Alluxio的配置主要在Hadoop配置文件中完成。


#### 在`core-site.xml`中设置属性

如果年你安装Flink的同时安装了Hadoop，将如下属性加到`core-site.xml`配置文件：

{% include Running-Flink-on-Alluxio/core-site-configuration.md %}

如果你没有安装Hadoop，创建一个包含以下内容的`core-site.xml`文件

{% include Running-Flink-on-Alluxio/create-core-site.md %}

#### 在`conf/flink-config.yaml`中指定`core-site.xml`的路径

接下来需要指定Flink中Hadoop配置的路径。打开Flink根目录下`conf/flink-config.yaml`文件，设置`fs.hdfs.hadoopconf`的值为`core-site.xml`的**目录**（对于新的Hadoop版本，该目录通常以`etc/hadoop`结尾）。 

#### 使Alluxio Client jar对Flink可用

最后一步，我们需要使Alluxio `jar`文件对Flink可用，该文件包含了配置好的`alluxio.hadoop.TFS`类。

具体做法有以下几种：

- 将`alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件放在Flink的`lib`目录下（对于本地模式和独立集群模式）。
- 将`alluxio-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件放在Flink的`ship`目录下（对于Flink安装在YARN上）。
- 在`HADOOP_CLASSPATH`环境变量中指定jar文件的路径（确保该环境变量在所有集群节点上有效）。举个例子：

{% include Running-Flink-on-Alluxio/hadoop-classpath.md %}

# 在Flink中使用Alluxio

Flink中使用Alluxio，指定路径时使用`alluxio://`前缀。

如果Alluxio是本地安装，有效路径类似于：
`alluxio://localhost:19998/user/hduser/gutenberg`。
