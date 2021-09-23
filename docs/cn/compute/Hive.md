---
layout: global
title: 在Alluxio上运行Apache Hive
nickname: Apache Hive
group: Compute Integrations
priority: 3
---

* 内容列表
{:toc}

该文档介绍如何运行[Apache Hive](http://hive.apache.org/)，以能够在不同存储层将Hive的表格存储到Alluxio当中。

## 前期准备

开始之前你需要安装好Java，同时使用[本地模式]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})或[集群模式]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }})构建好Alluxio。

接着[下载Hive](http://hive.apache.org/downloads.html)。

在Hadoop MapReduce上运行Hive之前，请按照[在Alluxio上运行MapReduce](Running-Hadoop-MapReduce-on-Alluxio.html)的指示来确保MapReduce可以运行在Alluxio上。

## 配置Hive

我们建议您从Alluxio[下载页面](http://www.alluxio.io/download)下载压缩包。或者，高级用户可以选择按照[这里](Building-Alluxio-From-Source.html#compute-framework-support)的说明来从源码编译这个客户端jar。Alluxio客户端jar可以在`{{site.ALLUXIO_CLIENT_JAR_PATH}}`找到。

在shell或`conf/hive-env.sh`中设置`HIVE_AUX_JARS_PATH`：

```console
$ export HIVE_AUX_JARS_PATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HIVE_AUX_JARS_PATH}
```


## 在Alluxio上创建Hive表

有不同的方法可以将Hive与Alluxio整合。这一节讨论的是如何将Alluxio作为文件系统的一员（像HDFS）来存储Hive表。这些表可以是[内部的或外部的](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ManagedandExternalTables)，新创建的表或HDFS中已存在的表。[下一节](Running-Hive-with-Alluxio.html#use-alluxio-as-default-filesystem)讨论的是如何将Alluxio作为Hive的默认文件系统。在接下来的部分，文档中的Hive运行在Hadoop MapReduce上。
*建议：接下来所有的Hive命令行例子同样适用于Hive Beeline。你可以在Beeline shell中尝试这些例子*

### 使用文件在Alluxio中创建新表

Hive可以使用存储在Alluxio中的文件来创建新表。设置非常直接并且独立于其他的Hive表。一个示例就是将频繁使用的Hive表存在Alluxio上，从而通过直接从内存中读文件获得高吞吐量和低延迟。

#### 创建新的内部表的Hive命令示例

这里有一个示例展示了在Alluxio上创建Hive的内部表。你可以从[http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/)下载数据文件（如：`ml-100k.zip`）。然后接下该文件，并且将文件`u.user`上传到Alluxio的`ml-100k/`下：

```console
$ ./bin/alluxio fs mkdir /ml-100k
$ ./bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio://master_hostname:port//ml-100k
```
然后创建新的内部表：

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

#### 创建新的外部表的Hive命令行示例

与前面的例子做同样的设置，然后创建一个新的外部表：

```
hive> CREATE EXTERNAL TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

区别是Hive会管理内部表的生命周期。
当你删除内部表，Hive会从Alluxio中将表的元数据以及数据文件都删掉。

现在你可以查询创建的表

```
hive> select * from u_user;
```

### 在ALluxio中使用已经存储在HDFS中的表

当Hive已经在使用并且管理着存储在HDFS中的表时，只要HDFS安装为Alluxio的底层存储系统，Alluxio也可以为Hive中的这些表提供服务。在这个例子中，我们假设HDFS集群已经安装为Alluxio根目录下的底层存储系统（例如，在`conf/alluxio-site.properties`中设置属性`alluxio.master.mount.table.root.ufs=hdfs://namenode:port/`）。请参考[统一命名空间](Unified-and-Transparent-Namespace.html)以获取更多关于安装操作的细节。

#### 使用已存在的内部表的Hive命令行示例

我们假设属性`hive.metastore.warehouse.dir`设置为默认值`/user/hive/warehouse`, 并且内部表已经像这样创建:

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

hive> LOAD DATA LOCAL INPATH '/path/to/ml-100k/u.user' OVERWRITE INTO TABLE u_user;
```

下面的HiveQL语句会将表数据的存储位置从HDFS转移到Alluxio中

```
hive> alter table u_user set location "alluxio://master_hostname:port/user/hive/warehouse/u_user";
```

验证表的位置是否设置正确:

```
hive> desc formatted u_user;
```

注意，第一次访问`alluxio://master_hostname:port/user/hive/warehouse/u_user`中的文件时会被认为是访问`hdfs://namenode:port/user/hive/warehouse/u_user`（默认的Hive内部数据存储位置）中对应的文件;一旦数据缓存在Alluxio中，在接下来的查询中Alluxio会使用这些缓存数据来服务查询而不用再一次从HDFS中读取数据。整个过程对于Hive和用户是透明的。

#### 使用已存在的外部表的Hive命令行示例

假设在Hive中有一个已存在的外部表`u_user` ，存储位置设置为`hdfs://namenode_hostname:port/ml-100k`.
你可以使用下面的HiveQL语句来检查它的“位置”属性

```
hive> desc formatted u_user;
```

然后使用下面的HiveQL语句将表数据的存储位置从HDFS转移到Alluxio中：

```
hive> alter table u_user set location "alluxio://master_hostname:port/ml-100k";
```

### 将表的元数据恢复到HDFS

在上面的两个关于将转移表数据的存储位置至Alluxio的例子中，你也可以将表的存储位置恢复到HDFS中：

```
hive> alter table TABLE_NAME set location "hdfs://namenode:port/table/path/in/HDFS";
```

### 使用已存在的Hive分区表
修改分区表的过程与修改非分区表非常相似，不同的地方在于除了改变表位置之外，我们还需要修改所有分区的分区位置。请参阅以下示例：
```
hive> alter table TABLE_NAME partition(PARTITION_COLUMN = VALUE) set location "hdfs://namenode:port/table/path/partitionpath";
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

若要启用容错模式，请在类路径中的`alluxio-site.properties`文件中适当地设置Alluxio群集属性（请参见下面的示例）。

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=[zookeeper_hostname]:2181
```

或者，您可以将属性添加到Hive`hive-site.xml`配置中，然后将其传播到Alluxio。

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

```console
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

![HiveTableInAlluxio]({{ site.baseurl }}/img/screenshot_hive_table_in_alluxio.png)

```
hive> select * from u_user;
```

你可以在命令行中看到相应查询结果：

![HiveQueryResult]({{ site.baseurl }}/img/screenshot_hive_query_result.png)
