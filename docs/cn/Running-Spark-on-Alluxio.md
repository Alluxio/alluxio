---
layout: global
title: 在Alluxio上运行Spark
nickname: Apache Spark
group: Frameworks
priority: 0
---

* 内容列表
{:toc}

该指南描述了如何在Alluxio上运行[Apache Spark](http://spark-project.org/)。HDFS作为一个分布式底层存储系统的一个例子。请注意，Alluxio除HDFS之外也支持其它的底层存储系统，计算框架(如Spark)可以通过Alluxio从任意数量的底层存储系统读写数据。

## 兼容性

Alluxio直接兼容Spark 1.1或更新版本而无需修改.

## 前期准备

### 一般设置

* Alluxio集群根据向导搭建完成(可以是[本地模式](Running-Alluxio-Locally.html)或者[集群模式](Running-Alluxio-on-a-Cluster.html))。

* Alluxio client需要在编译时指定Spark选项。在顶层`alluxio`目录中执行如下命令构建Alluxio:

{% include Running-Spark-on-Alluxio/spark-profile-build.md %}

* 请添加如下代码到`spark/conf/spark-defaults.conf`。

{% include Running-Spark-on-Alluxio/earlier-spark-version-bash.md %}

### 针对HDFS的额外设置

* 如果Alluxio运行Hadoop 1.x集群之上，创建一个新文件`spark/conf/core-site.xml`包含以下内容：

{% include Running-Spark-on-Alluxio/Hadoop-1.x-configuration.md %}


* 如果你使用zookeeper让Alluxio运行在容错模式并且Hadoop集群是1.x，添加如下内容到先前创建的`spark/conf/core-site.xml`:

{% include Running-Spark-on-Alluxio/fault-tolerant-mode-with-zookeeper-xml.md %}

以及如下内容到`spark/conf/spark-defaults.conf`:

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

Alluxio支持在给出具体的路径时，透明的从底层文件系统中取数据。将文件`LICENSE`放到Alluxio所挂载的目录下（默认是/alluxio）的HDFS中，意味着在这个目录下的HDFS中的任何文件都能被Alluxio发现。通过改变位于Server上的alluxio-env.sh文件中的 `ALLUXIO_UNDERFS_ADDRESS`属性可以修改这个设置。假定namenode节点运行在`localhost`，并且Alluxio默认的挂载目录是`alluxio`:

{% include Running-Spark-on-Alluxio/license-hdfs.md %}

注意：Alluxio没有文件的概念。你可以通过浏览web UI验证这点。在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

{% include Running-Spark-on-Alluxio/alluxio-hdfs-in-out-scala.md %}

打开浏览器，查看[http://localhost:19999/browse](http://localhost:19999/browse)。可以发现多出一个输出文件`LICENSE2`,
每一行是由文件`LICENSE`的对应行复制2次得到。并且，现在`LICENSE`文件出现在Alluxio文件系统空间。

注意：部分读取的块缓存默认是开启的，但如果已经将这个选项关闭的话，`LICENSE`文件很可能不在Alluxio存储（非In-Memory)中。这是因为Alluxio只存储完整读入块，如果文件太小，Spark作业中，每个executor读入部分块。为了避免这种情况，你可以在Spark中定制分块数目。对于这个例子，由于只有一个块，我们将设置分块数为1。

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

##在YARN上运行SPARK

为了最大化Spark作业所能达到数据本地化的数量，应当尽可能多地使用executor,希望至少每个节点拥有一个executor。按照Alluxio所有方法的部署，所有的计算节点上也应当拥有一个Alluxio worker。

当一个Spark作业在YARN上运行时,Spark启动executors不会考虑数据的本地化。之后Spark在决定怎样为它的executors分配任务时会正确地考虑数据的本地化。举例而言：
如果`host1`包含了`blockA`并且使用`blockA`的作业已经在YARN集群上以`--num-executors=1`的方式启动了,Spark会将唯一的executor放置在`host2`上，本地化就比较差。但是，如果以`--num-executors=2`的方式启动并且executors开始于`host1`和`host2`上,Spark会足够智能地将作业优先放置在`host1`上。

## Spark Shell中`Failed to login`问题
为了用Alluxio客户端运行Spark Shell，Alluxio客户端的jar包必须被添加到Spark driver和Spark executors的classpath中。可是有的时候Alluxio不能判断安全用户，从而导致类似于`Failed to login: No Alluxio User is found.`的错误。以下是一些解决方案。

### [建议] 为Spark 1.4.0以上版本配置`spark.sql.hive.metastore.sharedPrefixes`

这是建议的解决方案。

在Spark 1.4.0和之后的版本中，Spark为了访问hive元数据使用了独立的类加载器来加载java类。然而，这个独立的类加载器忽视了特定的包，并且让主类加载器去加载"共享"类（Hadoop的HDFS客户端就是一种"共享"类）。Alluxio客户端也应该由主类加载器加载，你可以将`alluxio`包加到配置参数`spark.sql.hive.metastore.sharedPrefixes`中，以通知Spark用主类加载器加载Alluxio。例如，该参数可以这样设置:

```bash
spark.sql.hive.metastore.sharedPrefixes=com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,alluxio
```

### [规避] 为Hadoop配置指定`fs.alluxio.impl`

如果以上的建议方案不可行，以下的方案可以规避掉这个问题。

为Hadoop配置指定`fs.alluxio.impl`也许可以帮助你解决该错误。`fs.alluxio.impl`应该被设置为`alluxio.hadoop.FileSystem`，如果你想使用Alluxio的容错机制，`fs.alluxio-ft.impl`应该被设置为`alluxio.hadoop.FaultTolerantFileSystem`。这里有几个选项来设置这些参数。

#### 更新SparkContext中的`hadoopConfiguration`

你可以在SparkContext中通过以下方式来更新Hadoop配置：

```scala
sc.hadoopConfiguration.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")
sc.hadoopConfiguration.set("fs.alluxio-ft.impl", "alluxio.hadoop.FaultTolerantFileSystem")
```

该操作应该在 `spark-shell`阶段早期，即Alluxio操作之前运行。

#### 更新Hadoop配置文件

你可以在Hadoop的配置文件中增加参数，并使Spark指向Hadoop的配置文件。Hadoop的`core-site.xml`中需要添加如下内容。

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
  <property>
    <name>fs.alluxio-ft.impl</name>
    <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  </property>
</configuration>
```

你可以通过在`spark-env.sh`设置`HADOOP_CONF_DIR`来使Spark指向Hadoop的配置文件。
