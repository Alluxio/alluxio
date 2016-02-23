---
layout: global
title: 在Alluxio上运行Spark
nickname: Apache Spark
group: Frameworks
priority: 0
---

该向导描述了如何在Alluxio上运行[Apache Spark](http://spark-project.org/)并且使用HDFS作为Alluxio底层存储系统。Alluxio除HDFS之外也支持其它的底层存储系统，计算框架(如Spark)可以通过Alluxio从底层存储系统读写数据。

## 兼容性

Alluxio直接兼容Spark 1.1或更新版本而无需修改.

## 前期准备

* Alluxio集群根据向导搭建完成(可以是[本地模式](Running-Alluxio-Locally.html)或者[集群模式](Running-Alluxio-on-a-Cluster.html))。
* 请添加如下代码到`spark/conf/spark-env.sh`。

{% include Running-Spark-on-Alluxio/earlier-spark-version-bash.md %}

* 如果Alluxio运行Hadoop 1.x集群之上，创建一个新文件`spark/conf/core-site.xml`包含以下内容：

{% include Running-Spark-on-Alluxio/Hadoop-1.x-configuration.md %}


* 如果你使用zookeeper让Alluxio运行在容错模式并且Hadoop集群是1.x，添加如下内容到先前创建的`spark/conf/core-site.xml`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-xml.md %}

以及如下内容到`spark/conf/spark-env.sh`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-bash.md %}

## 使用Alluxio作为输入输出

这一部分说明如何将Alluxio作为Spark应用的输入输出源。

将文件`foo`放到HDFS（假定namenode运行在`localhost`）:

{% include Running-Spark-on-Alluxio/foo.md %}

在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

{% include Running-Spark-on-Alluxio/Alluxio-In-Out-Scala.md %}

打开浏览器，查看[http://localhost:19999](http://localhost:19999)。可以发现多出一个输出文件`bar`,每一行是由文件`foo`的对应行复制2次得到。

当以容错模式运行Alluxio时，可以使用任何一个Alluxio master：

{% include Running-Spark-on-Alluxio/any-Alluxio-master.md %}

## 数据局部性

如果Spark任务的局部性应该是`NODE_LOCAL`而实际是`ANY`，可能是因为Alluxio和Spark使用了不同的网络地址表示，可能其中一个使用了主机名而另一个使用了IP地址。请参考 [this jira ticket](
https://issues.apache.org/jira/browse/SPARK-10149)获取更多细节（这里可以找到Spark社区的解决方案）。

提示:Alluxio使用主机名来表示网络地址，只有0.7.1版本使用了IP地址。Spark 1.5.x版本与Alluxio0.7.1做法一致，都使用了IP地址来表示网络地址，数据局部性不加修改即可使用。但是从0.8.0往后，为了与HDFS一致，Alluxio使用主机名表示网络地址。用户启动Spark时想要获取数据局部性，可以用Spark提供的如下脚本显式指定主机名。以slave-hostname启动Spark Worker:

{% include Running-Spark-on-Alluxio/slave-hostname.md %}

举例而言:

{% include Running-Spark-on-Alluxio/slave-hostname-example.md %}

也可以通过设置`$SPARK_HOME/conf/spark-env.sh`里的`SPARK_LOCAL_HOSTNAME`获取数据局部性。举例而言：

{% include Running-Spark-on-Alluxio/spark-local-hostname-example.md %}

用以上任何一种方法，Spark Worker的地址变为主机名并且局部性等级变为NODE_LOCAL，如下Spark WebUI所示：

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})

