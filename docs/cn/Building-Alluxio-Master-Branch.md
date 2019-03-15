---
layout: global
title: 构建Alluxio主分支
nickname: 构建主分支
group: Resources
---

* 内容列表
{:toc}

该指南介绍如何从头编译Alluxio。

这部分内容的前提条件是你已安装[Java 7 (或以上)](Java-Setup.html)、[Maven](Maven.html)以及[Thrift 0.9.3](Thrift.html) (可选)。

从Github上获取主分支并打包：

{% include Building-Alluxio-Master-Branch/checkout.md %}

若出现`java.lang.OutOfMemoryError: Java heap space`，请执行：

{% include Building-Alluxio-Master-Branch/OutOfMemoryError.md %}

若需要构建一个特定的版本Alluxio，例如{{site.ALLUXIO_RELEASED_VERSION}}，先执行`cd alluxio`，接着执行`git checkout v{{site.ALLUXIO_RELEASED_VERSION}}`。

Maven构建环境将自动获取依赖，编译源码，运行单元测试，并进行打包。如果你是第一次构建该项目，下载依赖包可能需要一段时间，但以后的构建过程将会快很多。

一旦构建完成，执行以下命令启动Alluxio：

{% include Common-Commands/start-alluxio.md %}

若要确认Alluxio是否在运行，可以访问[http://localhost:19999](http://localhost:19999)，或者查看`alluxio/logs`目录下的日志文件，也可以执行下面的简单程序:

{% include Common-Commands/runTests.md %}

若正确运行，应能看到类似以下输出结果：

{% include Building-Alluxio-Master-Branch/test-result.md %}

执行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}

## 单元测试

运行所有单元测试：

{% include Building-Alluxio-Master-Branch/unit-tests.md %}

在本地文件系统之外的存储层上运行所有单元测试：

{% include Building-Alluxio-Master-Branch/under-storage.md %}

目前支持的`<under-storage-profile>`值包括：

{% include Building-Alluxio-Master-Branch/supported-values.md %}

若需将日志输出到STDOUT，将以下命令追加到`mvn`命令后：

{% include Building-Alluxio-Master-Branch/STDOUT.md %}

## 计算框架支持
针对不同的计算框架构建Alluxio，可以使用不同的计算配置文件运行Maven构建。 生成的Alluxio客户端位于                                                        `{{site.ALLUXIO_CLIENT_JAR_PATH_BUILD}}`.

### Hadoop

你可以运行以下命令以使用Hadoop编译Alluxio。

```bash
mvn install -P<HADOOP_PROFILE> -DskipTests
```

对于不同的Hadoop发行版，可用的Hadoop配置文件包括`hadoop-1`, `hadoop-2.2`, `hadoop-2.3` ... `hadoop-2.8`。通过查看[这节](#发行版支持)，你可以进一步设置特定的Hadoop发行版来编译。



### Spark
你可以运行以下命令以使用Spark编译Alluxio。

```bash
mvn install -Pspark -DskipTests
```

### Flink

你可以运行以下命令以使用Flink编译Alluxio。

```bash
mvn install -Pflink -DskipTests
```

### Presto
你可以运行以下命令以使用Presto编译Alluxio。

```bash
mvn install -Ppresto -DskipTests
```

## 发行版支持

要针对不同hadoop发行版构建Alluxio，只需修改  
`hadoop.version`。你可以运行以下命令：

```bash
mvn install -P<HADOOP_PROFILE> -Dhadoop.version=<HADOOP_VERSION> -DskipTests
```
其中`<HADOOP_VERSION>`可以根据不同的发行版设置

### Apache

由于所有主要构建版本都来自Apache，因此所有Apache发行版可以直接使用

{% include Building-Alluxio-Master-Branch/Apache.md %}

### Cloudera

对于Cloudera发行版，使用该形式`$apacheRelease-cdh$cdhRelease`的版本号

{% include Building-Alluxio-Master-Branch/Cloudera.md %}

### MapR

对于MapR发行版，其值为

{% include Building-Alluxio-Master-Branch/MapR.md %}

### Pivotal

对于Pivotal发行版，使用`$apacheRelease-gphd-$pivotalRelease`形式的版本号

{% include Building-Alluxio-Master-Branch/Pivotal.md %}

### Hortonworks

对于Hortonworks发行版，使用`$apacheRelease.$hortonRelease`形式的版本号

{% include Building-Alluxio-Master-Branch/Hortonworks.md %}

## 系统设置

有时为了在本地通过单元测试，需要进行些系统设置，常用的一个设置项为ulimit。

### Mac

为增加允许的文件以及进程数目，执行以下命令：

{% include Building-Alluxio-Master-Branch/increase-number.md %}

推荐将Alluxio本地资源从Spotlight索引中移除，否则你的Mac机器在单元测试中可能会尝试重复对文件系统构建索引从而挂起。若要进行移除，进入`System Preferences > Spotlight > Privacy`，点击`+`按钮，浏览Alluxio的本地资源目录，并点击`Choose`将其添加到排除列表。
