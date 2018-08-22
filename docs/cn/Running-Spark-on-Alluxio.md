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

* 我们建议您从Alluxio[下载页面](http://www.alluxio.org/download)下载tarball.
  另外，高级用户可以选择根据[这里](Building-Alluxio-From-Source.html#compute-framework-support)的说明将源代码编译为客户端jar包，并在本文余下部分使用于`{{site.ALLUXIO_CLIENT_JAR_PATH}}`路径处生成的jar包。

* 为使Spark应用程序能够在Alluxio中读写文件， 必须将Alluxio客户端jar包分布在不同节点的应用程序的classpath中（每个节点必须使客户端jar包具有相同的本地路径`{{site.ALLUXIO_CLIENT_JAR_PATH}}`）

* 请添加如下代码到`spark/conf/spark-defaults.conf`。

```bash
spark.driver.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### 针对HDFS的额外设置

* 如果Alluxio运行Hadoop 1.x集群之上，创建一个新文件`spark/conf/core-site.xml`包含以下内容：

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

* 如果你使用zookeeper让Alluxio运行在容错模式，添加如下内容到`${SPARK_HOME}/conf/spark-defaults.conf`:

```bash
spark.driver.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
spark.executor.extraJavaOptions -Dalluxio.zookeeper.address=zookeeperHost1:2181,zookeeperHost2:2181 -Dalluxio.zookeeper.enabled=true
```
或者你可以添加内容到先前创建的Hadoop配置文件中`${SPARK_HOME}/conf/core-site.xml`:

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>[zookeeper_hostname]:2181</value>
  </property>
</configuration>
```

## 检查Spark与Alluxio的集成性 (支持Spark 2.X)

在Alluxio上运行Spark之前，你需要确认你的Spark配置已经正确设置集成了Alluxio。Spark集成检查器可以帮助你确认。

当你运行Saprk集群(或单机运行)时,你可以在Alluxio项目目录运行以下命令:

```bash
$ integration/checker/bin/alluxio-checker.sh spark <spark master uri> [partition number]
```

这里`partition number`是一个可选参数。
你可以使用`-h`来显示关于这个命令的有用信息。这条命令将报告潜在的问题，可能会阻碍你在Alluxio上运行Spark。
这将会报告你在Alluxio上运行Spark会遇到的潜在问题。


## 使用Alluxio作为输入输出

这一部分说明如何将Alluxio作为Spark应用的输入输出源。

### 使用已经存于Alluxio的数据

首先，我们将从Alluxio文件系统中拷贝一些本地数据。将文件`LICENSE`放到Alluxio（假定你正处在Alluxio工程目录下）:

```bash
$ bin/alluxio fs copyFromLocal LICENSE /LICENSE
```

在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```

打开浏览器，查看[http://localhost:19999/browse](http://localhost:19999/browse)。可以发现多出一个输出文件`LICENSE2`,
每一行是由文件`LICENSE`的对应行复制2次得到。

### 使用来自HDFS的数据

Alluxio支持在给出具体的路径时，透明的从底层文件系统中取数据。将文件`LICENSE`放到Alluxio所挂载的目录下（默认是`/alluxio`）的HDFS中，意味着在这个目录下的HDFS中的任何文件都能被Alluxio发现。通过改变位于Server上的`conf/alluxio-site.properties`文件中的 `alluxio.underfs.address`属性可以修改这个设置。假定namenode节点运行在`localhost`，并且Alluxio默认的挂载目录是`alluxio`:

```bash
$ hadoop fs -put -f /alluxio/LICENSE hdfs://localhost:9000/alluxio/LICENSE
```

注意：Alluxio没有文件的概念。你可以通过浏览web UI验证这点。在`spark-shell`中运行如下命令（假定Alluxio Master运行在`localhost`）:

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```

打开浏览器，查看[http://localhost:19999/browse](http://localhost:19999/browse)。可以发现多出一个输出文件`LICENSE2`,
每一行是由文件`LICENSE`的对应行复制2次得到。并且，现在`LICENSE`文件出现在Alluxio文件系统空间。

注意：部分读取的块缓存默认是开启的，但如果已经将这个选项关闭的话，`LICENSE`文件很可能不在Alluxio存储（非In-Alluxio)中。这是因为Alluxio只存储完整读入块，如果文件太小，Spark作业中，每个executor读入部分块。为了避免这种情况，你可以在Spark中定制分块数目。对于这个例子，由于只有一个块，我们将设置分块数为1。

```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE", 1)
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```

### 使用容错模式

当以容错模式运行Alluxio时，可以使用任何一个Alluxio master：

```scala
> val s = sc.textFile("alluxio://stanbyHost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://activeHost:19998/LICENSE2")
```

## 数据本地化

如果Spark任务的定位应该是`NODE_LOCAL`而实际是`ANY`，可能是因为Alluxio和Spark使用了不同的网络地址表示，可能其中一个使用了主机名而另一个使用了IP地址。请参考 [this jira ticket](
https://issues.apache.org/jira/browse/SPARK-10149)获取更多细节（这里可以找到Spark社区的解决方案）。

提示:Alluxio使用主机名来表示网络地址，只有0.7.1版本使用了IP地址。Spark 1.5.x版本与Alluxio0.7.1做法一致，都使用了IP地址来表示网络地址，数据本地化不加修改即可使用。但是从0.8.0往后，为了与HDFS一致，Alluxio使用主机名表示网络地址。用户启动Spark时想要获取数据本地化，可以用Spark提供的如下脚本显式指定主机名。以slave-hostname启动Spark Worker:

```bash
$ $SPARK_HOME/sbin/start-slave.sh -h <slave-hostname> <spark master uri>
```

举例而言:

```bash
$ $SPARK_HOME/sbin/start-slave.sh -h simple30 spark://simple27:7077
```

也可以通过设置`$SPARK_HOME/conf/spark-env.sh`里的`SPARK_LOCAL_HOSTNAME`获取数据本地化。举例而言：

```properties
SPARK_LOCAL_HOSTNAME=simple30
```

用以上任何一种方法，Spark Worker的地址变为主机名并且定位等级变为NODE_LOCAL，如下Spark WebUI所示：

![hostname]({{site.data.img.screenshot_datalocality_sparkwebui}})

![locality]({{site.data.img.screenshot_datalocality_tasklocality}})

### 在YARN上运行SPARK

为了最大化Spark作业所能达到数据本地化的数量，应当尽可能多地使用executor,希望至少每个节点拥有一个executor。按照Alluxio所有方法的部署，所有的计算节点上也应当拥有一个Alluxio worker。

当一个Spark作业在YARN上运行时,Spark启动executors不会考虑数据的本地化。之后Spark在决定怎样为它的executors分配任务时会正确地考虑数据的本地化。举例而言：
如果`host1`包含了`blockA`并且使用`blockA`的作业已经在YARN集群上以`--num-executors=1`的方式启动了,Spark会将唯一的executor放置在`host2`上，本地化就比较差。但是，如果以`--num-executors=2`的方式启动并且executors开始于`host1`和`host2`上,Spark会足够智能地将作业优先放置在`host1`上。

## `class alluxio.hadoop.FileSystem not found` 与SparkSQL和Hive MetaStore有关的问题
为了用Alluxio客户端运行Spark Shell，Alluxio客户端的jar包必须如[之前描述](Running-Spark-on-Alluxio.html#general-setup)的那样，被添加到Spark driver和Spark executors的classpath中。可是有的时候Alluxio不能判断安全用户，从而导致类似于下面的错误。

```
org.apache.hadoop.hive.ql.metadata.HiveException: MetaException(message:java.lang.RuntimeException: java.lang.ClassNotFoundException: Class alluxio.hadoop.FileSystem not found)
```

推荐的解决方案是为Spark 1.4.0以上版本配置[`spark.sql.hive.metastore.sharedPrefixes`](http://spark.apache.org/docs/2.0.0/sql-programming-guide.html#interacting-with-different-versions-of-hive-metastore).
在Spark 1.4.0和之后的版本中，Spark为了访问hive元数据使用了独立的类加载器来加载java类。然而，这个独立的类加载器忽视了特定的包，并且让主类加载器去加载"共享"类（Hadoop的HDFS客户端就是一种"共享"类）。Alluxio客户端也应该由主类加载器加载，你可以将`alluxio`包加到配置参数`spark.sql.hive.metastore.sharedPrefixes`中，以通知Spark用主类加载器加载Alluxio。例如，该参数可以这样设置:

```bash
spark.sql.hive.metastore.sharedPrefixes=com.mysql.jdbc,org.postgresql,com.microsoft.sqlserver,oracle.jdbc,alluxio
```

## `java.io.IOException: No FileSystem for scheme: alluxio` 与在YARN上运行Spark有关的问题


如果你在YARN上使用基于Alluxio的Spark并遇到异常`java.io.IOException：No FileSystem for scheme：alluxio`，请将以下内容添加到 `${SPARK_HOME}/conf/core-site.xml`：

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```
