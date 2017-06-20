---
layout: global
title: 在Alluxio上运行Apache Hive
nickname: Apache Hive
group: Frameworks
priority: 2
---

* 内容列表
{:toc}

该文档介绍如何运行[Apache Hive](http://hive.apache.org/)，以能够在不同存储层将Hive的表格存储到Alluxio当中。

## 前期准备

开始之前你需要安装好[Java](Java-Setup.html)，同时使用[本地模式](Running-Alluxio-Locally.html)或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

接着[下载Hive](http://hive.apache.org/downloads.html)。

## 配置

在Hadoop MapReduce上运行Hive之前，请按照[在Alluxio上运行MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html)的指示来确保MapReduce可以运行在Alluxio上。

Hive用户可以创建[外部表](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ExternalTables)，令其指向Alluxio中的特定位置，而使其他表的存储不变，或者使用Alluxio作为默认的文件系统。下面我们将介绍两种在Alluxio上使用Hive的方法。

## 创建位于Alluxio上的外部表

Hive可以创建存储在Alluxio上的外部表。设置很直接，并且独立于其他的Hive表。一个示例就是将频繁使用的Hive表存在Alluxio上，从而通过直接从内存中读文件获得高吞吐量和低延迟。

### 配置Hive

在shell中或`conf/hive-env.sh`下设置`HIVE_AUX_JARS_PATH`：

```bash
export HIVE_AUX_JARS_PATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HIVE_AUX_JARS_PATH}
```
###Hive 命令示例

这里有一个示例展示了在Alluxio上创建Hive的外部表。你可以从[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/)下载数据文件（如：`ml-100k.zip`）。然后接下该文件，并且将文件`u.user`上传到Alluxio的`ml-100k/`下：

```bash
$ bin/alluxio fs mkdir /ml-100k
$ bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port//ml-100k
```
然后创建外部表：

```
hive> CREATE EXTERNAL TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
OW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

## Alluxio作为默认文件系统

Apache Hive也可以使用Alluxio，只需通过一个一般的文件系统接口来替换Hadoop文件系统使用Alluxio。这种方式下，Hive使用Alluxio作为其默认文件系统，它的元数据和中间结果都将存储在Alluxio上。

### 配置 Hive

添加以下配置项到你的Hive安装目下的`conf`目录里的`hive-site.xml`中：

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://master_hostname:port</value>
</property>
```

若要启用容错模式，将Alluxio模式设置为`alluxio-ft`：

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio-ft:///</value>
</property>
```

### 添加额外Alluxio配置到Hive中

如果你有其他需要对Hive指定的Alluxio配置属性，将它们添加到每个结点的Hadoop配置目录下`core-site.xml`中。例如，将`alluxio.user.file.writetype.default`
属性由默认的`MUST_CACHE`修改成`CACHE_THROUGH`：

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

### 在Alluxio上运行Hive

在Alluxio中为Hive创建相应目录：

```bash
$ ./bin/alluxio fs mkdir /tmp
$ ./bin/alluxio fs mkdir /user/hive/warehouse
$ ./bin/alluxio fs chmod 775 /tmp
$ ./bin/alluxio fs chmod 775 /user/hive/warehouse
```

接着你可以根据[Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted)来使用Hive了。

### Hive命令行示例

在Hive中创建表并且将本地文件加载到Hive中：

依然使用来自[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/)的数据文件`ml-100k.zip`。

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

hive> LOAD DATA LOCAL INPATH '/path/to/ml-100k/u.user'
OVERWRITE INTO TABLE u_user;
```

在浏览器中输入`http://master_hostname:port`以访问Alluxio Web UI，你可以看到相应文件夹以及Hive创建的文件：

![HiveTableInAlluxio]({{site.data.img.screenshot_hive_table_in_alluxio}})

```
hive> select * from u_user;
```

你可以在命令行中看到相应查询结果：

![HiveQueryResult]({{site.data.img.screenshot_hive_query_result}})
