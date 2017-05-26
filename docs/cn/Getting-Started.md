---
layout: global
title: 快速上手指南
group: User Guide
priority: 0
---

* 内容列表
{:toc}

体验Alluxio最简单的方式是单机本地安装。在这个快速上手指南里，我们会引导你在本地机器上安装Alluxio，挂载样本数据，对Alluxio中的数据执行一些基本操作。具体来说，包括:

* 下载和配置Alluxio
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
* [Java 7或更新版本](Java-Setup.html)
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

这会创建一个包含所有的Alluxio源文件和Java二进制文件的文件夹`alluxio-{{site.ALLUXIO_RELEASED_VERSION}}`。

## 配置Alluxio

在开始使用Alluxio之前，我们需要配置它。大部分使用默认设置即可。

从模板文件创建`conf/alluxio-env.sh`配置文件。也可以通过如下命令创建配置文件：

```bash
$ ./bin/alluxio bootstrapConf localhost
```

### [奖励] AWS相关配置

如果你有一个包含access key id和secret accsee key的AWS账户，你可以添加你的Alluxio配置以备接下来与Amazon S3的交互。如下命令可以添加你的AWS访问信息到`conf/alluxio-site.properties`文件。

```bash
$ echo "aws.accessKeyId=AWS_ACCESS_KEY_ID" >> conf/alluxio-site.properties
$ echo "aws.secretKey=AWS_SECRET_ACCESS_KEY" >> conf/alluxio-site.properties
```

你必须将**AWS_ACCESS_KEY_ID**替换成你的AWS access key id，将**AWS_SECRET_ACCESS_KEY**替换成你的AWS secret access key。现在，Alluxio完全配置好了。

## 启动Alluxio

接下来，我们格式化Alluxio为启动Alluxio做准备。如下命令会格式化Alluxio的日志和worker存储目录，以便接下来启动master和worker。

```bash
$ ./bin/alluxio format
```

现在，我们启动Alluxio！Alluxio默认配置成在localhost启动master和worker。我们可以用如下命令在localhost启动Alluxio：

```bash
$ ./bin/alluxio-start.sh local
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

可惜现在Alluxio里没有文件。我们可以拷贝文件到Alluxio。`copyFromLocal`命令可以拷贝本地文件到Alluxio中。

```bash
$ ./bin/alluxio fs copyFromLocal LICENSE /LICENSE
Copied LICENSE to /LICENSE
```

拷贝`LICENSE`文件之后，我们可以在Alluxio中看到它。列出Alluxio里的文件：

```bash
$ ./bin/alluxio fs ls /
26.22KB   06-20-2016 11:30:04:415  In Memory      /LICENSE
```
输出显示`LICENSE`文件在Alluxio中，也包含一些其他的有用信息，比如文件的大小，创建的日期，文件的in-memory状态。

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

现在，如果我们再次检查UFS。文件出现了。

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
87.86KB   06-20-2016 12:50:51:660  Not In Memory  /mnt/s3/sample_tweets_100k.csv
933.21KB  06-20-2016 12:50:53:633  Not In Memory  /mnt/s3/sample_tweets_1m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
9.61MB    06-20-2016 12:50:55:821  Not In Memory  /mnt/s3/sample_tweets_10m.csv
```

我们也可以[在web UI上看见新挂载的文件和目录](http://localhost:19999/browse?path=%2Fmnt%2Fs3)。

通过Alluxio统一命名空间，你可以无缝地从不同存储系统中交互数据。举个例子，使用`ls`shell命令，你可以递归地列举出一个目录下的所有文件。

```bash
$ ./bin/alluxio fs ls -R /
26.22KB   06-20-2016 11:30:04:415  In Memory      /LICENSE
1.00B     06-20-2016 12:28:39:176                 /mnt
4.00B     06-20-2016 12:30:41:986                 /mnt/s3
87.86KB   06-20-2016 12:50:51:660  Not In Memory  /mnt/s3/sample_tweets_100k.csv
933.21KB  06-20-2016 12:50:53:633  Not In Memory  /mnt/s3/sample_tweets_1m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
9.61MB    06-20-2016 12:50:55:821  Not In Memory  /mnt/s3/sample_tweets_10m.csv
```

输出显示了Alluxio文件系统根目录下来源于挂载的不同文件系统的所有文件。`/LICENSE`文件在本地文件系统，`/mnt/s3/`目录下是S3的文件。

## [奖励]用Alluxio加速数据访问

由于Alluxio利用内存存储数据，它可以加速数据的访问。首先，我们看一看Alluxio中文件的状态(从S3中挂载)。

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
149.77MB  06-20-2016 12:50:55:473  Not In Memory  /mnt/s3/sample_tweets_150m.csv
```

输出显示了文件**Not In Memory**。该文件是tweet的样本。我们看看有多少tweet提到了单词“kitten”。使用如下的命令，我们可以统计含有“kitten”的tweet数量。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c kitten
889

real	0m22.857s
user	0m7.557s
sys	0m1.181s
```

取决于你的网络连接状况，该操作可能会超过20秒。如果读取文件时间过长，你可以选择一个小一点的数据集。该目录下的其他文件是该文件的更小子集。

现在，让我们看一下有多少tweet包含单词"puppy"。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m25.998s
user	0m6.828s
sys	0m1.048s
```

正如你看见的，每个命令花费大量的时间访问数据。Alluxio使用内存存储这些数据来加速数据访问。`cat`命令不能缓存数据到Alluxio内存中。另外一个命令`load`可以告诉Alluxio存储数据到内存中。你可以使用如下命令告诉Alluxio加载数据到内存中。

```bash
$ ./bin/alluxio fs load /mnt/s3/sample_tweets_150m.csv
```

加载完文件之后，使用`ls`命令检查其状态：

```bash
$ ./bin/alluxio fs ls /mnt/s3/sample_tweets_150m.csv
149.77MB  06-20-2016 12:50:55:473  In Memory      /mnt/s3/sample_tweets_150m.csv
```

输出显示文件**In Memory**。既然文件在内存中，读文件应该会更快。

我们统计一下包含"puppy"的tweet数量。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c puppy
1553

real	0m1.917s
user	0m2.306s
sys	0m0.243s
```

如你所见，读文件非常快，只有几秒！由于数据在Alluxio内存中，你可以再次很快的读取文件。我们统计一下包含单词"bunny"的tweet数量。

```bash
$ time ./bin/alluxio fs cat /mnt/s3/sample_tweets_150m.csv | grep -c bunny
907

real	0m1.983s
user	0m2.362s
sys	0m0.240s
```

恭喜！你在本地安装了Alluxio并且通过Alluxio加速了数据访问！

## 关闭Alluxio

你完成了本地安装和使用Alluxio，你可以使用如下命令关闭Alluxio：

```bash
$ ./bin/alluxio-stop.sh local
```

## 结论

恭喜你完成了Alluxio的快速上手指南！你成功地在本地电脑上下载和安装Alluxio，并且通过Alluxio shell进行了基本的交互。这是一个如何上手Alluxio的简单例子。

除此之外还有很多可以学习。你可以通过我们的文档学到Alluxio的各种特性。你可以在你的环境中安装Alluxio，挂载你已有的存储系统到Alluxio，配置你的应用和Alluxio一起工作。更多的资源在下面。

### 部署Alluxio

Alluxio可以部署在很多不同的环境下。

* [本地运行Alluxio](Running-Alluxio-Locally.html)
* [在Virtual Box上运行Alluxio](Running-Alluxio-on-Virtual-Box.html)
* [在集群上独立运行Alluxio](Running-Alluxio-on-a-Cluster.html)
* [Alluxio独立模式实现容错](Running-Alluxio-Fault-Tolerant.html)
* [在EC2上运行Alluxio](Running-Alluxio-on-EC2.html)
* [在GCE上运行Alluxio](Running-Alluxio-on-GCE.html)
* [在EC2上使用Mesos运行Alluxio](Running-Alluxio-on-Mesos.html)
* [在EC2上运行带容错机制的Alluxio](Running-Alluxio-Fault-Tolerant-on-EC2.html)
* [在EC2上使用YARN运行Alluxio](Running-Alluxio-on-EC2-Yarn.html)

### 底层存储系统

有很多可以通过Alluxio访问的底层存储系统。

* [Alluxio使用GCS](Configuring-Alluxio-with-GCS.html)
* [Alluxio使用S3](Configuring-Alluxio-with-S3.html)
* [Alluxio使用Swift](Configuring-Alluxio-with-Swift.html)
* [Alluxio使用GlusterFS](Configuring-Alluxio-with-GlusterFS.html)
* [Alluxio使用HDFS](Configuring-Alluxio-with-HDFS.html)
* [Alluxio使用MapR-FS](Configuring-Alluxio-with-MapR-FS.html)
* [Alluxio使用Secure HDFS](Configuring-Alluxio-with-secure-HDFS.html)
* [Alluxio使用OSS](Configuring-Alluxio-with-OSS.html)
* [Alluxio使用NFS](Configuring-Alluxio-with-NFS.html)

### 框架和应用

不同的框架和应用和Alluxio整合。

* [Apache Spark使用Alluxio](Running-Spark-on-Alluxio.html)
* [Apache Hadoop MapReduce使用Alluxio](Running-Hadoop-MapReduce-on-Alluxio.html)
* [Apache Flink使用Alluxio](Running-Flink-on-Alluxio.html)
* [Apache Zeppelin使用Alluxio](Accessing-Alluxio-from-Zeppelin.html)
* [Apache HBase使用Alluxio](Running-HBase-on-Alluxio.html)
