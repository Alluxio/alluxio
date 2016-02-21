---
layout: global
title: 在Alluxio上运行Spark
nickname: Apache Spark
group: Frameworks
priority: 0
---

该向导描述了如何在Alluxio上运行[Apache Spark](http://spark-project.org/)并且使用HDFS作为Alluxio底层存储系统。Alluxio除HDFS之外也支持其它的底层存储系统，计算框架(如Spark)可以通过Alluxio从底层存储系统读写数据。

## 兼容性

如果Spark和Alluxio的版本符合如下的配对关系，它们之间的兼容性将达到最佳。

<table class="table table-striped">
<tr><th>Spark版本</th><th>Alluxio版本</th></tr>
{% for item in site.data.table.versions-of-Spark-and-Alluxio %}

<tr>
  <td>{{item.Spark-Version}}</td>
  <td>{{item.Alluxio-Version}}</td>
</tr>
{% endfor %}
</table>

如果Spark的版本默认不支持你的Alluxio安装版本（比如，你在某些旧的Spark版本上安装最新版本的Alluxio），你可以通过更新alluxio-core-client中Spark依赖的版本来重新编译Spark。具体做法是：编辑`spark/core/pom.xml`，将`alluxio-core-client`的依赖版本改成`your_alluxio_version`:

{% include Running-Spark-on-Alluxio/your_Alluxio_version.md %}

## 前期准备

* 确保你的Spark与Alluxio版本兼容，具体内容参考[兼容性](#compatibility)部分。
* Alluxio集群根据向导搭建完成(可以是[本地模式](Running-Alluxio-Locally.html)或者[集群模式](Running-Alluxio-on-a-Cluster.html))。
* 如果Spark版本低于`1.0.0`，请添加如下代码到`spark/conf/spark-env.sh`。

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

## 持久化Spark RDD到Alluxio

这个特性需要Spark1.0或更高版本，Alluxio0.4.1或更高版本。请参考[Spark向导](http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence)获取更多有关持久化RDD的细节。

为了持久化Spark RDD，你的Spark程序需设置两个参数:
`spark.externalBlockStore.url`和`spark.externalBlockStore.baseDir`.

* `spark.externalBlockStore.url` 是Alluxio文件系统的URL。默认是`alluxio://localhost:19998`。
* `spark.externalBlockStore.baseDir`是Alluxio文件系统存储RDD的基础目录。可以是以逗号分隔的多个Alluxio目录的列表。默认是`java.io.tmpdir`。

为了持久化RDD到Alluxio，需要传递`StorageLevel.OFF_HEAP`参数。下面的例子是通过Spark shell传递的：

{% include Running-Spark-on-Alluxio/off-heap-Spark-shell.md %}

在Spark应用运行时，通过Alluxio Web UI（默认是[http://localhost:19999](http://localhost:19999)）可以查看`spark.externalBlockStore.baseDir`。你会看见很多文件，是已经持久化的RDD块。目前版本中，当Spark应用完成时，这些文件将会被清理。

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

