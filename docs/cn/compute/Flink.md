---
layout: global
title: 在Alluxio上运行Apache Flink
nickname: Apache Flink
group: Compute Integrations
priority: 2
---

* 内容列表
{:toc}

该指南介绍如何在Alluxio上运行[Apache Flink](http://flink.apache.org/),以便你在Flink中使用Alluxio的文件。

## 前期准备

* 安装好Java 8 Update 161 (8u161+)或更新版本, 64-bit.。
* 使用[本地模式]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})或[集群模式]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }})构建好Alluxio。
* 请在[Apache Flink](http://flink.apache.org/)网站上阅读Flink安装说明。

## 配置

Apache Flink可以通过通用文件系统包装类（可用于Hadoop文件系统）来使用Alluxio。因此，Alluxio的配置主要在Hadoop配置文件中完成。


### 在`core-site.xml`中设置属性

如果你安装Flink的同时安装了Hadoop，将如下属性加到`core-site.xml`配置文件：

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

如果你没有安装Hadoop，创建一个包含以下内容的`core-site.xml`文件

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

### 在`conf/flink-conf.yaml`中指定`core-site.xml`的路径

接下来需要指定Flink中Hadoop配置的路径。打开Flink根目录下`conf/flink-conf.yaml`文件，设置`fs.hdfs.hadoopconf`的值为`core-site.xml`的**目录**（对于新的Hadoop版本，该目录通常以`etc/hadoop`结尾）。

### 布置Alluxio客户端Jar包

为了与Alluxio通信，需要提供带有Alluxio核心客户端Jar包的Flink程序。我们推荐您直接从[http://www.alluxio.io/download](http://www.alluxio.io/download)下载压缩包。另外，高级用户可以选择用源文件编译产生客户端Jar包。遵循以下步骤：[here]({{ '/en/contributor/Building-Alluxio-From-Source.html' | relative_url }}),在 `{{site.ALLUXIO_CLIENT_JAR_PATH_BUILD}}`路径下可以找到客户端的Jar包。

接下来需要让Alluxio `jar`文件对Flink可用，因为其中包含了配置好的`alluxio.hadoop.FileSystem`类。

有以下几种方式实现：

- 将`{{site.ALLUXIO_CLIENT_JAR_PATH}}`文件放在Flink的`lib`目录下（对于本地模式以及独立集群模式）。
- 将`{{site.ALLUXIO_CLIENT_JAR_PATH}}`文件放在布置在Yarn中的Flink下的`ship`目录下。
- 在`HADOOP_CLASSPATH`环境变量中指定该jar文件的路径（要保证该路径对集群中的所有节点都有效）。例如：

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### 将Alluxio额外属性转化为Flink属性

除此以外，如果`conf/alluxio-site.properties`和客户端相关的配置文件中有任何指定的属性，请在`{FLINK_HOME}/conf/flink-conf
.yaml`文件中将这些属性转化为`env.java.opts`，从而方便Flink使用Alluxio的配置。例如，如果你想要将CACHE_THROUGH作为Alluxio客户端的写文件方式
，你应该在 `{FLINK_HOME}/conf/flink-conf.yaml`增加如下配置

```yaml
env.java.opts: -Dalluxio.user.file.writetype.default=CACHE_THROUGH
```

## 在Flink中使用Alluxio

Flink中使用Alluxio，指定路径时使用`alluxio://`前缀。

如果Alluxio是本地安装，有效路径类似于：
`alluxio://localhost:19998/user/hduser/gutenberg`。

### Wordcount示例

该示例假定你已经按前文指导安装了Alluxio和Flink。

将`LICENSE`文件放入Alluxio中，假定当前目录为Alluxio工程的根目录：

```console
$ ./bin/alluxio fs copyFromLocal LICENSE alluxio://localhost:19998/LICENSE
```

在Flink工程的根目录下运行以下命令：

```console
$ ./bin/flink run examples/batch/WordCount.jar \
--input alluxio://localhost:19998/LICENSE \
--output alluxio://localhost:19998/output
```

接着打开浏览器，进入[http://localhost:19999/browse](http://localhost:19999/browse)，其中应存在一个`output`文件，该文件即为对`LICENSE`文件进行word count的结果。
