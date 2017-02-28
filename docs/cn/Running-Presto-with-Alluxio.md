---
layout: global
title: 在Alluxio上运行Facebook Presto
nickname: Presto
group: Frameworks
priority: 2
---

该文档介绍如何运行[Facebook Presto](https://prestodb.io/)，让Presto能够查询Alluxio上的Hive表。

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)，Java 版本要求在1.8以上，同时使用[本地模式](Running-Alluxio-Locally.html)
或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

Alluxio客户端须要䅂Presto㤫具体配置文件一起编译。在顶层目录'alluxio'是用下面的命令编译整个项目

```bash
mvn clean package -Ppresto -DskipTests
```

接着[下载Presto](https://repo1.maven.org/maven2/com/facebook/presto/presto-server/)。并且已经配置好
[Hive On Alluxio](http://www.alluxio.org/docs/master/cn/Running-Hive-with-Alluxio.html)

# 配置

Presto 通过连接Hive metastore来获取数据库和表的信息，同时通过表的元数据信息来获取表数据所在的hdfs位置信息。
所以需要先配置[Presto on Hdfs](https://prestodb.io/docs/current/installation/deployment.html),为了访问hdfs，
需要将hadoop的core-site.xml、hdfs-site.xml加入到Presto，并通过 hive.config.resources 指向hadoop的配置文件.

#### 配置`core-site.xml`

你需要向你的Presto目录里的`core-site.xml`中添加以下配置项：

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

# 分发Alluxio客户端jar包

将Alluxio客户端Jar包分发到Presto所有节点中：
- 因为Presto使用的guava版本是18.0，而Alluxio使用的是14.0，所以需要将Alluxio client端的pom.xml中guava版本修改为18.0并重新编译Alluxio客户端。

- 你必须将Alluxio客户端jar包 `alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
（在`/<PATH_TO_ALLUXIO>/core/client/target/`目录下）放置在所有Presto节点的`$PRESTO_HOME/plugin/hadoop/`
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
/home/path/presto/presto-cli-0.159-executable.jar --server masterIp:prestoPort --execute "use default;select * from u_user limit 10;" --user username --debug
```

你可以在命令行中看到相应查询结果：

![PrestoQueryResult]({{site.data.img.screenshot_presto_query_result}})

日志：

![PrestoQueryLog]({{site.data.img.screenshot_presto_query_log}})
