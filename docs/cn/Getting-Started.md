---
layout: global
title: 快速上手指南
group: Home
priority: 3
---

* 内容列表
{:toc}

体验Alluxio最简单的方式是单机本地安装。在这个快速上手指南里，我们会引导你在本地机器上安装Alluxio，挂载样本数据，对Alluxio中的数据执行一些基本操作。具体来说，包括:

* 下载和配置Alluxio
* 验证Alluxio运行环境
* 本地启动Alluxio
* 通过Alluxio Shell进行基本的文件操作
* **[奖励]** 挂载一个公开的Amazon S3 bucket到Alluxio上
* 关闭Alluxio

**[奖励]** 如果你有一个[包含access key id和secret accsee key的AWS账户](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSGettingStartedGuide/AWSCredentials.html)，你可以完成额外的任务。需要你AWS账户信息的章节都有**[奖励]**的标签。

**注意** 本指南旨在让你快速开始与Alluxio系统进行交互。Alluxio在大数据工作负载的分布式环境中表现最好。这些特性都难以适用于本地环境。如果你有兴趣运行一个更大规模的、能够突出Alluxio性能优势的例子，可以选择这两个白皮书中的任一个，尝试其中的指南：[Accelerating
on-demand data analytics with Alluxio](https://alluxio.com/resources/accelerating-on-demand-data-analytics-with-alluxio)、[Accelerating data analytics on ceph object storage with Alluxio](https://www.alluxio.com/blog/accelerating-data-analytics-on-ceph-object-storage-with-alluxio)。

## 前期准备

为了接下来的快速上手指南，你需要:

* Mac OS X或Linux
* [Java 8或更新版本](Java-Setup.html)
* **[奖励]** AWS账户和秘钥

### 安装SSH(Mac OS X)

如果你使用Mac OS X，你必须能够ssh到localhost。远程登录开启方法：打开**系统偏好设置**,然后打开**共享**，确保**远程登录**已开启。

## 下载Alluxio

首先，[下载Alluxio发布版本](http://www.alluxio.org/download)。你可以从[Alluxio下载页面](http://www.alluxio.org/download)下载最新的兼容不同Hadoop版本的{{site.ALLUXIO_RELEASED_VERSION}}预编译版。

接着，你可以用如下命令解压下载包。取决于你下载的预编译二进制版本，你的文件名可能和下面有所不同。

```bash
$ tar -xzf alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-bin.tar.gz
$ cd alluxio-{{site.ALLUXIO_RELEASED_VERSION}}
```

这会创建一个包含所有的Alluxio源文件和Java二进制文件的文件夹`alluxio-{{site.ALLUXIO_RELEASED_VERSION}}`。通过这个教程，这个文件夹的路径将被引用为`${ALLUXIO_HOME}`。

## 配置Alluxio

在开始使用Alluxio之前，我们需要配置它。大部分使用默认设置即可。

在`${ALLUXIO_HOME}/conf`目录下，根据模板文件创建`conf/alluxio-site.properties`配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

在`conf/alluxio-site.properties`文件中将 `alluxio.master.hostname`更新为你打算运行Alluxio Master的机器主机名。

```bash
$ echo "alluxio.master.hostname=localhost" >> conf/alluxio-site.properties
```
### [奖励] AWS相关配置

如果你有一个包含access key id和secret accsee key的AWS账户，你可以添加你的Alluxio配置以备接下来与Amazon S3的交互。如下命令可以添加你的AWS访问信息到`conf/alluxio-site.properties`文件。

```bash
$ echo "aws.accessKeyId=<AWS_ACCESS_KEY_ID>" >> conf/alluxio-site.properties
$ echo "aws.secretKey=<AWS_SECRET_ACCESS_KEY>" >> conf/alluxio-site.properties
```

你必须将**<AWS_ACCESS_KEY_ID>**替换成你的AWS access key id，将**<AWS_SECRET_ACCESS_KEY>**替换成你的AWS secret access key。现在，Alluxio完全配置好了。

## 验证Alluxio运行环境

在启动Alluxio前，我们要保证当前系统环境下Alluxio可以正常运行。我们可以通过运行如下命令来验证Alluxio的本地运行环境:

```bash
$ ./bin/alluxio validateEnv local
```

该命令将汇报在本地环境运行Alluxio可能出现的问题。如果你配置Alluxio运行在集群中，并且你想要验证所有节点的运行环境，你可以运行如下命令:

```bash
$ ./bin/alluxio validateEnv all
```

我们也可以使用该命令运行某些特定验证项目。例如，

```bash
$ ./bin/alluxio validateEnv local ulimit
```

将只运行验证本地系统资源限制方面的项目。

你可以在[这里](Developer-Tips.html)查看更多关于本命令的信息。

## 启动Alluxio

接下来，我们格式化Alluxio为启动Alluxio做准备。如下命令会格式化Alluxio的日志和worker存储目录，以便接下来启动master和worker。

```bash
$ ./bin/alluxio format
```

现在，我们启动Alluxio！Alluxio默认配置成在localhost启动master和worker。我们可以用如下命令在localhost启动Alluxio：

```bash
$ ./bin/alluxio-start.sh local SudoMount
```

恭喜！Alluxio已经启动并运行了！你可以访问[http://localhost:19999](http://localhost:19999)查看Alluxio master的运行状态，访问[http://localhost:30000](http://localhost:30000)查看Alluxio worker的运行状态。

## 使用Alluxio Shell

既然Alluxio在运行，我们可以通过[Alluxio shell](Command-Line-Interface.html)检查Alluxio文件系统。Alluxio shell包含多种与Alluxio交互的命令行操作。你可以通过如下命令调用Alluxio shell：

```bash
$ ./bin/alluxio fs
```

该命令将打印可用的Alluxio命令行操作。

你可以通过`ls`命令列出Alluxio里的文件。比如列出根目录下所有文件：

```bash
$ ./bin/alluxio fs ls /
```

不过现在Alluxio里没有文件。我们可以拷贝文件到Alluxio。`copyFromLocal`命令可以拷贝本地文件到Alluxio中。

```bash
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

拷贝`LICENSE`文件之后，我们可以在Alluxio中看到它。列出Alluxio里的文件：

```bash
$ ./bin/alluxio fs ls /
-rw-r--r-- staff  staff     26847 NOT_PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
```
输出显示`LICENSE`文件在Alluxio中，也包含一些其他的有用信息，比如文件的大小，创建的日期，文件的所有者和组，以及Alluxio中这个文件的占比。

你也可以通过Alluxio shell来查看文件的内容。`cat`命令可以打印文件的内容。

```bash
$ ./bin/alluxio fs cat /LICENSE
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
...
```

默认设置中，Alluxio使用本地文件系统作为底层文件系统(UFS)。默认的UFS路径是`./underFSStorage`。我们可以查看UFS中的内容：

```bash
$ ls ./underFSStorage/
```

然而，这个目录不存在！这是由于Alluxio默认只写入数据到Alluxio存储空间，而不会写入UFS。

但是，我们可以告诉Alluxio将文件从Alluxio空间持久化到UFS。shell命令`persist`可以做到。

```bash
$ ./bin/alluxio fs persist /LICENSE
persisted file /LICENSE with size 26847
```

如果我们现在再次检查UFS，文件就会出现。

```bash
$ ls ./underFSStorage
LICENSE
```

如果我们在[master webUI](http://localhost:19999/browse)中浏览Alluxio文件系统，我们可以看见`LICENSE`文件以及其它有用的信息。这里，**Persistence State**栏显示文件为**PERSISTED**。

## [奖励]Alluxio中的挂载

Alluxio通过统一命名空间的特性统一了对底层存储的访问。你可以阅读[统一命名空间的博客](http://www.alluxio.com/2016/04/unified-namespace-allowing-applications-to-access-data-anywhere/)和[统一命名空间的文档](Unified-and-Transparent-Namespace.html)获取更详细的解释。

这个特性允许用户挂载不同的存储系统到Alluxio命名空间中并且通过Alluxio命名空间无缝地跨存储系统访问文件。

首先，我们在Alluxio中创建一个目录作为挂载点。

```bash
$ ./bin/alluxio fs mkdir /mnt
Successfully created directory /mnt
```

接着，我们挂载一个已有的S3 bucket样本到Alluxio。你可以使用我们提供的S3 bucket样本。

```bash
$ ./bin/alluxio fs mount -readonly alluxio://localhost:19998/mnt/s3 s3a://alluxio-quick-start/data
Mounted s3a://alluxio-quick-start/data at alluxio://localhost:19998/mnt/s3
```

现在，S3 bucket已经挂载到Alluxio命名空间中了。

我们可以通过Alluxio命名空间列出S3中的文件。使用熟悉的`ls`shell命令列出S3挂载目录下的文件。

```bash
$ ./bin/alluxio fs ls /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

我们也可以[在web UI上看见新挂载的文件和目录](http://localhost:19999/browse?path=%2Fmnt%2Fs3)。

通过Alluxio统一命名空间，你可以无缝地从不同存储系统中交互数据。举个例子，使用`ls`shell命令，你可以递归地列举出一个目录下的所有文件。

```bash
$ ./bin/alluxio fs ls -R /
-rw-r--r-- staff  staff     26847 PERSISTED 01-09-2018 15:24:37:088 100% /LICENSE
drwxr-xr-x staff  staff         1 PERSISTED 01-09-2018 16:05:59:547  DIR /mnt
dr-x------ staff  staff         4 PERSISTED 01-09-2018 16:34:55:362  DIR /mnt/s3
-r-x------ staff  staff    955610 PERSISTED 01-09-2018 16:35:00:882   0% /mnt/s3/sample_tweets_1m.csv
-r-x------ staff  staff  10077271 PERSISTED 01-09-2018 16:35:00:910   0% /mnt/s3/sample_tweets_10m.csv
-r-x------ staff  staff     89964 PERSISTED 01-09-2018 16:35:00:972   0% /mnt/s3/sample_tweets_100k.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

输出显示了Alluxio文件系统根目录下来源于挂载的不同文件系统的所有文件。`/LICENSE`文件在本地文件系统，`/mnt/s3/`目录下是S3的文件。

## [奖励]用Alluxio加速数据访问

由于Alluxio利用内存存储数据，它可以加速数据的访问。首先，我们看一看Alluxio中一个文件的状态(从S3中挂载)。

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002   0% /mnt/s3/sample_tweets_150m.csv
```

输出显示了文件**Not In Memory**。该文件是微博的样本。我们看看有多少微博提到了单词“kitten”。使用如下的命令，我们可以统计含有“kitten”的tweet数量。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c kitten
889

real	0m22.857s
user	0m7.557s
sys	0m1.181s
```

取决于你的网络连接状况，该操作可能会超过20秒。如果读取文件时间过长，你可以选择一个小一点的数据集。该目录下的其他文件是该文件的更小子集。
如你所见，每个访问数据的命令都需要花费较长的时间。通过将数据放在内存中，Alluxio可以提高访问数据的速度。

在通过`cat`命令获取文件后，你可以用`ls`命令查看文件的状态：

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
-r-x------ staff  staff 157046046 PERSISTED 01-09-2018 16:35:01:002 100% /mnt/s3/sample_tweets_150m.csv
```

输出显示文件已经100%被加载到Alluxio中，既然如此，那么再次访问该文件的速度应该会快很多。

让我们来统计一下拥有“puppy”这个单词的微博的数目。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

如你所见，读文件的速度非常快，仅仅需要数秒钟！并且，因为数据已经存放到了Alluxio中了，你可以轻易的以较快的速度再次读该文件。
现在让我们来统计一下有多少微博包含“bunny”这个词。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

恭喜！你在本地安装了Alluxio并且通过Alluxio加速了数据访问！

## 中止Alluxio

你完成了本地安装和使用Alluxio，你可以使用如下命令中止Alluxio：

```bash
$ ./bin/alluxio-stop.sh local
```

## 总结

恭喜你完成了Alluxio的快速上手指南！你成功地在本地电脑上下载和安装Alluxio，并且通过Alluxio shell进行了基本的交互。这是一个如何上手Alluxio的简单例子。

除此之外还有很多可以学习。你可以通过我们的文档学到Alluxio的各种特性。你可以在你的环境中安装Alluxio，挂载你已有的存储系统到Alluxio，配置你的应用和Alluxio一起工作。浏览下方以获得更多信息。

### 部署Alluxio

Alluxio可以部署在很多不同的环境下。

* [本地运行Alluxio](Running-Alluxio-Locally.html)
* [在集群上独立运行Alluxio](Running-Alluxio-on-a-Cluster.html)
* [在Virtual Box中运行Alluxio](Running-Alluxio-on-Virtual-Box.html)
* [在Docker中运行Alluxio](Running-Alluxio-On-Docker.html)
* [在EC2上运行Alluxio](Running-Alluxio-on-EC2.html)
* [在GCE上运行Alluxio](Running-Alluxio-on-GCE.html)
* [在EC2上使用Mesos运行Alluxio](Running-Alluxio-on-Mesos.html)
* [在EC2上运行带容错机制的Alluxio](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [Alluxio与YARN整合运行](Running-Alluxio-Yarn-Integration.html)
* [在YARN Standalone模式上运行Alluxio](Running-Alluxio-Yarn-Standalone.html)

### 底层存储系统

有很多可以通过Alluxio访问的底层存储系统。

* [Alluxio使用Azure Blob Store](Configuring-Alluxio-with-Azure-Blob-Store.html)
* [Alluxio使用S3](Configuring-Alluxio-with-S3.html)
* [Alluxio使用GCS](Configuring-Alluxio-with-GCS.html)
* [Alluxio使用Minio](Configuring-Alluxio-with-Minio.html)
* [Alluxio使用Ceph](Configuring-Alluxio-with-Ceph.html)
* [Alluxio使用Swift](Configuring-Alluxio-with-Swift.html)
* [Alluxio使用GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [Alluxio使用MapR-FS](Configuring-Alluxio-with-MapR-FS.html)
* [Alluxio使用HDFS](Configuring-Alluxio-with-HDFS.html)
* [Alluxio使用Secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [Alluxio使用OSS](Configuring-Alluxio-with-OSS.html)
* [Alluxio使用NFS](Configuring-Alluxio-with-NFS.html)

### 框架和应用

不同的框架和应用和Alluxio整合。

* [Apache Spark使用Alluxio](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce使用Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache HBase使用Alluxio](Running-HBase-on-Alluxio.html)
* [Apache Flink使用Alluxio](Running-Flink-on-Alluxio.html)
* [Presto 使用 Alluxio](Running-Presto-with-Alluxio.html)
* [Apache Hive 使用 Alluxio](Running-Hive-with-Alluxio.html)
* [Apache Zeppelin使用Alluxio](Accessing-Alluxio-from-Zeppelin.html)
