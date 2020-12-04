---
layout: global
title: 在Alluxio上运行Hadoop MapReduce
nickname: Apache Hadoop MapReduce
group: Compute Integrations
priority: 2
---

* 内容列表
{:toc}

该文档介绍如何让Alluxio与Apache Hadoop MapReduce一起运行，从而让你轻松地运行文件存储在Alluxio上的MapReduce程序。

## 初始化设置

该文档的先决条件包括：
- 你已安装了Java。
- 你已经按照这些文档建立了一个Alluxio集群: [本地模式]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})或[集群模式]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }})。
- 为了运行一些简单的map-reduce实例，我们也推荐你下载根据你的hadoop版本对应的[map-reduce examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-examples/2.4.1)，或者如果你正在使用Hadoop 1，下载这个[examples jar](http://mvnrepository.com/artifact/org.apache.hadoop/hadoop-examples/1.2.1)。

## 编译Alluxio客户端

为了使MapReduce应用可以与Alluxio进行通信，你需要将Alluxio Client的Jar包包含在MapReduce的classpaths中。我们建议你从Alluxio [download page](http://www.alluxio.io/download) 下载压缩包。

同时，高级用户可以选择使用源代码来编译生成Alluxio Client的Jar包。
你可以运行以下命令[编译Alluxio源代码]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}#compute-framework-support)。

新的Alluxio客户端Jar包可以在`{{site.ALLUXIO_CLIENT_JAR_PATH}}`中发现。

## 配置Hadoop

将以下两个属性添加到Hadoop的安装目录下的`core-site.xml`文件中：

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x)</description>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```

该配置让你的MapReduce作用在输入和输出文件中通过Alluxio scheme `alluxio://` 来识别URIs。

其次, 在`conf`目录中`hadoop-env.sh`文件中修改`$HADOOP_CLASSPATH`：

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

该配置确保Alluxio客户端jar包是利用的，对于通过Alluxio的URIs来创建和提交作业进行交互的MapReduce作业客户端。

## 分发Alluxio客户端Jar包

为了让MapRedude应用在Alluxio上读写文件，Alluxio客户端Jar包必须被分发到不同节点的应用相应的classpaths中。

[如何从Cloudera上加入第三方库](http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/)这篇文档介绍了分发Jar包的多种方式。文档中建议通过使用命令行的`-libjars`选项，使用分布式缓存来分发Alluxio客户端Jar包。另一种分发客户端Jar包的方式就是手动将其分发到Hadoop节点上。下面就是这两种主流方法的介绍：

1.**使用-libjars命令行选项**

你可以在使用`hadoop jar ...`的时候加入-libjars命令行选项，指定`{{site.ALLUXIO_CLIENT_JAR_PATH}}`为`-libjars`的参数。这条命令会把该Jar包放到Hadoop的DistributedCache中，使所有节点都可以访问到。例如，下面的命令就是将Alluxio客户端Jar包添加到`-libjars`选项中。

```console
$ ./bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} <INPUT FILES> <OUTPUT DIRECTORY>
```

有时候，你还需要设置环境变量`HADOOP_CLASSPATH`，让Alluxio客户端在运行hadoop jar命令时创建的客户端JVM可以使用jar包：

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

2.**手动将Client Jar包分发到所有节点**

为了在每个节点安装Alluxio,将客户端jar包`{{site.ALLUXIO_CLIENT_JAR_PATH}}`置于每个MapReduce节点的`$HADOOP_HOME/lib`（由于版本不同也可能是`$HADOOP_HOME/share/hadoop/common/lib`），然后重新启动Hadoop。
另一种选择，在你的Hadoop部署中，把这个jar包添加到`mapreduce.application.classpath`系统属性，确保jar包在classpath上。
为了在每个节点上安装Alluxio，将客户端Jar包`mapreduce.application.classpath`，该方法要注意的是所有Jar包必须再次安装，因为每个Jar包都更新到了最新版本。另一方面，当该Jar包已经在每个节点上的时候，就没有必要使用`-libjars`命令行选项了。

## 在本地模式的Alluxio上运行Hadoop wordcount

为了方便，我们假设是伪分布式的集群，通过运行如下命令启动(根据hadoop的版本，你可能需要把`./bin`换成`./sbin`)：

```console
$ cd $HADOOP_HOME
$ ./bin/stop-all.sh
$ ./bin/start-all.sh
```

以本地模式启动Alluxio：

```console
$ ./bin/alluxio-start.sh local SudoMount
```

你可以在Alluxio中加入两个简单的文件来运行wordcount。在你的Alluxio目录中运行：

```console
$ ./bin/alluxio fs copyFromLocal LICENSE /wordcount/input.txt
```

该命令将`LICENSE`文件复制到Alluxio的文件命名空间中，并指定其路径为`/wordcount/input.txt`。

现在我们运行一个用于wordcount的MapReduce作业。

```console
$ ./bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} \
alluxio://localhost:19998/wordcount/input.txt \
alluxio://localhost:19998/wordcount/output
```

作业完成后，wordcount的结果将存在Alluxio的`/wordcount/output`目录下。你可以通过运行如下命令来查看结果文件：

```console
$ ./bin/alluxio fs ls /wordcount/output
$ ./bin/alluxio fs cat /wordcount/output/part-r-00000
```
