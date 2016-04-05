---
layout: global
title: 在Alluxio上运行Spark
nickname: Apache Spark
group: Frameworks
priority: 0
---

该指南描述了如何在Alluxio上运行[Apache Spark](http://spark-project.org/)。HDFS作为一个分布式底层存储系统的一个例子。请注意，Alluxio除HDFS之外也支持其它的底层存储系统，计算框架(如Spark)可以通过Alluxio从任意数量的底层存储系统读写数据。

## 兼容性

Alluxio直接兼容Spark 1.1或更新版本而无需修改.

## 前期准备

### 一般设置

* Alluxio集群根据向导搭建完成(可以是[本地模式](Running-Alluxio-Locally.html)或者[集群模式](Running-Alluxio-on-a-Cluster.html))。

* Alluxio client需要在编译时指定Spark选项。在顶层`alluxio`目录中执行如下命令构建Alluxio:

{% include Running-Spark-on-Alluxio/spark-profile-build.md %}

* 请添加如下代码到`spark/conf/spark-env.sh`。

{% include Running-Spark-on-Alluxio/earlier-spark-version-bash.md %}

###针对HDFS的额外设置

* 如果Alluxio运行Hadoop 1.x集群之上，创建一个新文件`spark/conf/core-site.xml`包含以下内容：

{% include Running-Spark-on-Alluxio/Hadoop-1.x-configuration.md %}


* 如果你使用zookeeper让Alluxio运行在容错模式并且Hadoop集群是1.x，添加如下内容到先前创建的`spark/conf/core-site.xml`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-xml.md %}

以及如下内容到`spark/conf/spark-env.sh`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-bash.md %}

## 使用Alluxio作为输入输出

这一部分说明如何将Alluxio作为Spark应用的输入输出源。

### 使用已经存于Alluxio的数据

首先，我们将从Alluxio文件系统中拷贝一些本地数据。将文件`LICENSE`放到Alluxio（假定你正处在Alluxio工程目录下）:

{% include Running-Spark-on-Alluxio/license-local.md %}

在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

{% include Running-Spark-on-Alluxio/alluxio-local-in-out-scala.md %}

打开浏览器，查看[http://localhost:19999/browse](http://localhost:19999/browse)。可以发现多出一个输出文件`LICENSE2`,
每一行是由文件`LICENSE`的对应行复制2次得到。

### 使用来自HDFS的数据
 
Alluxio支持在给出具体的路径时，透明的从底层文件系统中取数据。将文件`LICENSE`放到HDFS中（假定namenode节点运行在`localhost`，并且Alluxio工程目录是`alluxio`）:

{% include Running-Spark-on-Alluxio/license-hdfs.md %}

注意：Alluxio没有文件的概念。你可以通过浏览web UI验证这点。在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

{% include Running-Spark-on-Alluxio/alluxio-hdfs-in-out-scala.md %}

打开浏览器，查看[http://localhost:19999/browse](http://localhost:19999/browse)。可以发现多出一个输出文件`LICENSE2`,
每一行是由文件`LICENSE`的对应行复制2次得到。并且，现在`LICENSE`文件出现在Alluxio文件系统空间。

注意：`LICENSE`文件很可能不在Alluxio存储（不是In-Memory)。这是因为Alluxio只存储完整读入块，如果文件太小，Spark作业中，每个executor读入部分块。为了避免这种情况，你可以在Spark中定制分块数目。对于这个例子，由于只有一个块，我们将设置分块数为1。

  {% include Running-Spark-on-Alluxio/alluxio-one-partition.md %}

### 使用容错模式

当以容错模式运行Alluxio时，可以使用任何一个Alluxio master：

{% include Running-Spark-on-Alluxio/any-Alluxio-master.md %}

## 数据本地化

如果Spark任务的定位应该是`NODE_LOCAL`而实际是`ANY`，可能是因为Alluxio和Spark使用了不同的网络地址表示，可能其中一个使用了主机名而另一个使用了IP地址。请参考 [this jira ticket](
https://issues.apache.org/jira/browse/SPARK-10149)获取更多细节（这里可以找到Spark社区的解决方案）。

提示:Alluxio使用主机名来表示网络地址，只有0.7.1版本使用了IP地址。Spark 1.5.x版本与Alluxio0.7.1做法一致，都使用了IP地址来表示网络地址，数据本地化不加修改即可使用。但是从0.8.0往后，为了与HDFS一致，Alluxio使用主机名表示网络地址。用户启动Spark时想要获取数据本地化，可以用Spark提供的如下脚本显式指定主机名。以slave-hostname启动Spark Worker:

{% include Running-Spark-on-Alluxio/slave-hostname.md %}

举例而言:

{% include Running-Spark-on-Alluxio/slave-hostname-example.md %}

也可以通过设置`$SPARK_HOME/conf/spark-env.sh`里的`SPARK_LOCAL_HOSTNAME`获取数据本地化。举例而言：

{% include Running-Spark-on-Alluxio/spark-local-hostname-example.md %}

用以上任何一种方法，Spark Worker的地址变为主机名并且定位等级变为NODE_LOCAL，如下Spark WebUI所示：

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})

