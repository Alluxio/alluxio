---
layout: global
title: 在Alluxio上运行Presto
nickname: Presto
group: Frameworks
priority: 2
---

该文档介绍如何运行[Presto](https://prestodb.io/)，让Presto能够查询Alluxio上的Hive表。

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)，Java 版本要求在1.8以上，同时使用[本地模式](Running-Alluxio-Locally.html)
或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

Alluxio客户端需要和Presto的具体配置文件一起编译。在顶层目录'alluxio'下使用下面的命令编译整个项目

```bash
mvn clean package -Ppresto -DskipTests
```

接着[下载Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)(此文档使用0.170版本)。并且已经配置好
[Hive On Alluxio](http://www.alluxio.org/docs/master/cn/Running-Hive-with-Alluxio.html)

# 配置

Presto 通过连接Hive metastore来获取数据库和表的信息，同时通过表的元数据信息来获取表数据所在的hdfs位置信息。
所以需要先配置[Presto on Hdfs](https://prestodb.io/docs/current/installation/deployment.html),为了访问hdfs，
需要将hadoop的core-site.xml、hdfs-site.xml加入到Presto每个节点的设置文件`/<PATH_TO_PRESTO>/etc/catalog/hive.properties`中的`hive.config.resources`的值.

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
HA模式的Alluxio需要加入如下配置
```xml
<property>
  <name>fs.alluxio-ft.impl</name>
  <value>alluxio.hadoop.FaultTolerantFileSystem</value>
  <description>The Alluxio FileSystem (Hadoop 1.x and 2.x) with fault tolerant support</description>
</property>
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

此外，我们建议提高`alluxio.user.network.netty.timeout.ms`的值（比如10分钟），来防止读异地大文件时的超时问题。

#### 提高`hive.max-split-size`值

Presto的Hive集成里使用了配置[`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html)来控制一个查询的分布式并行粒度。我们建议将这个值提高到你的Alluxio的块大小以上，以防止Presto在同一个块上进行多个并行的查找带来的相互阻塞。

# 分发Alluxio客户端jar包

将Alluxio客户端Jar包分发到Presto所有节点中：
- 因为Presto使用的guava版本是18.0，而Alluxio使用的是14.0，所以需要将Alluxio client端的pom.xml中guava版本修改为18.0并重新编译Alluxio客户端。

- 你必须将Alluxio客户端jar包 `{{site.ALLUXIO_CLIENT_JAR_PATH}}`放置在所有Presto节点的`$PRESTO_HOME/plugin/hive-hadoop2/`
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

在presto client执行如下查询：

```
/home/path/presto/presto-cli-0.170-executable.jar --server masterIp:prestoPort --execute "use default;select * from u_user limit 10;" --user username --debug
```

你可以在命令行中看到相应查询结果：

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})

日志：

![PrestoQueryLog]({{site.data.img.screenshot_presto_query_log}})
