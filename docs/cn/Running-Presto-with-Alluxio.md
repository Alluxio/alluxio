---
layout: global
title: 在Alluxio上运行Presto
nickname: Presto
group: Frameworks
priority: 2
---

该文档介绍如何运行[Presto](https://prestodb.io/)，让Presto能够查询存储在Alluxio上的Hive表。

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)，Java 版本要求在1.8以上，同时使用[本地模式](Running-Alluxio-Locally.html)
或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

接着[下载Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)(此文档使用0.191版本)。并且请使用
[Hive On Alluxio](Running-Hive-with-Alluxio.html)完成Hive初始化。

# 配置

Presto 从Hive metastore中获取数据库和表元数据的信息，同时通过表的元数据信息条目来获取表数据所在的hdfs位置信息。
所以需要先配置[Presto on HDFS](https://prestodb.io/docs/current/installation/deployment.html),为了访问HDFS，
需要将Hadoop的core-site.xml、hdfs-site.xml加入到Presto每个节点的设置文件`/<PATH_TO_PRESTO>/etc/catalog/hive.properties`中的`hive.config.resources`的值.

#### 配置`core-site.xml`

你需要向你的`hive.properties`指向的`core-site.xml`中添加以下配置项：

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>fs.AbstractFileSystem.alluxio.impl</name>
  <value>alluxio.hadoop.AlluxioFileSystem</value>
  <description>The Alluxio AbstractFileSystem (Hadoop 2.x)</description>
</property>
```

要使用容错模式，需要在classpath的`alluxio-site.properties`文件中正确的配置Alluxio集群的属性：

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=[zookeeper_hostname]:2181
```

或者，你可以将属性添加到Hadoop`core-site.xml`配置中，然后将其传播到Alluxio

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

#### 配置额外的Alluxio配置

类似于上面的配置方法，额外的Alluxio设置可以添加到每个节点上Hadoop目录下的`core-site.xml`文件里。比如可以如此来将`alluxio.user.file.writetype.default`从默认值`MUST_CACHE`改为`CACHE_THROUGH`:

```xml
<property>
 <name>alluxio.user.file.writetype.default</name>
 <value>CACHE_THROUGH</value>
</property>
```

另外，你也可以将[`alluxio-site.properties`](Configuration-Settings.html)的路径追加到Presto JVM配置中，该配置在Presto目录下的`etc/jvm.config`文件中。该方法的好处是只需在`alluxio-site.properties`配置文件中设置所有Alluxio属性。

```bash
...
-Xbootclasspath/p:<path-to-alluxio-site-properties>
```

此外，我们建议提高`alluxio.user.network.netty.timeout`的值（比如10分钟），来防止读远程worker中的大文件时的超时问题。

#### 使能 `hive.force-local-scheduling`

推荐您组合使用Presto和Alluxio，这样Presto工作节点能够从本地获取数据。Presto中一个重要的需要使能的选项是`hive.force-local-scheduling`,使能该选项能使得数据分片被调度到恰好处理该分片的Alluxio工作节点上。默认情况下，Presto中`hive.force-local-scheduling`设置为false，并且Presto也不会尝试将工作调度到Alluxio节点上。

#### 提高`hive.max-split-size`值

Presto的Hive集成里使用了配置[`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html)来控制一个查询的分布式并行粒度。我们建议将这个值提高到你的Alluxio的块大小以上，以防止Presto在同一个块上进行多个并行的查找带来的相互阻塞。

# 分发Alluxio客户端jar包

推荐您从[http://www.alluxio.org/download](http://www.alluxio.org/download)下载压缩包。另外，高阶用户可以按照指导[here](Building-Alluxio-Master-Branch.html#compute-framework-support)从源码编译这个客户端jar包。在路径`{{site.ALLUXIO_CLIENT_JAR_PATH}}`下可以找到Alluxio客户端jar包。

将Alluxio客户端Jar包分发到Presto所有节点中：
- 你必须将Alluxio客户端jar包 `{{site.ALLUXIO_CLIENT_JAR_PATH_PRESTO}}`放置在所有Presto节点的`$PRESTO_HOME/plugin/hive-hadoop2/`
目录中（针对不同hadoop版本，放到相应的文件夹下），并且重启所有coordinator和worker。

# Presto命令行示例

在Hive中创建表并且将本地文件加载到Hive中：

你可以从[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/)下载数据文件。

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

hive> LOAD DATA LOCAL INPATH '<path_to_ml-100k>/u.user'
OVERWRITE INTO TABLE u_user;
```

在浏览器中输入`http://master_hostname:19999`以访问Alluxio Web UI，你可以看到相应文件夹以及Hive创建的文件：

![HiveTableInAlluxio]({{site.data.img.screenshot_presto_table_in_alluxio}})

你可以通过[说明](Running-Hive-with-Alluxio.html#create-new-tables-from-files-in-alluxio)创建已存在在Alluxio中的表。

之后，在presto client执行如下查询：

```
/home/path/presto/presto-cli-0.191-executable.jar --server masterIp:prestoPort --execute "use default;select * from u_user limit 10;" --user username --debug
```

你可以在命令行中看到相应查询结果：

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})

日志：

![PrestoQueryLog]({{site.data.img.screenshot_presto_query_log}})
