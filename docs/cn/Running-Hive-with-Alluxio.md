---
layout: global
title: 在Alluxio上运行Apache Hive
nickname: Apache Hive
group: Frameworks
priority: 2
---

该文档介绍如何运行[Apache Hive](http://hive.apache.org/)，以能够在不同存储层将Hive的表格存储到Alluxio当中。

# 前期准备

开始之前你需要安装好[Java](Java-Setup.html)，同时使用[本地模式](Running-Alluxio-Locally.html)或[集群模式](Running-Alluxio-on-a-Cluster.html)构建好Alluxio。

接着[下载Hive](http://hive.apache.org/downloads.html)。

# 配置

Apache Hive允许你通过Hadoop通用文件系统接口使用Alluxio，因此要使用Alluxio作为存储系统，主要是配置Hive以及其底层计算框架。

#### 配置`hive-site.xml`

你需要向你的Hive安装目下的`conf`目录里的`hive-site.xml`中添加以下配置项：

```xml
<property>
   <name>fs.defaultFS</name>
   <value>alluxio://<master_hostname>:19998</value>
</property>
```

#### 配置Hadoop MapReduce

如果你在Hadoop MapReduce上运行Hive，那么Hive能够从Hadoop的配置文件中读取相应配置。另外，Hive的Hadoop作业会将其中间结果存储在Alluxio中。

需要在Hadoop下的`core-site.xml`文件中添加以下配置项：


```xml
<property>
   <name>fs.alluxio.impl</name>
   <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
   <name>fs.AbstractFileSystem.alluxio.impl</name>
   <value>alluxio.hadoop.AlluxioFileSystem</value>
</property>
```

在`hadoop-env.sh`文件中修改`$HADOOP_CLASSPATH`：

```bash
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

将Alluxio客户端Jar包分发到Hadoop MapReduce所有节点中：

- 你必须将Alluxio客户端jar包`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`
（在`/<PATH_TO_ALLUXIO>/core/client/target/`目录下）放置在所有MapReduce节点的`$HADOOP_HOME/share/hadoop/common/lib`
目录中（对某些Hadoop版本，可能是`$HADOOP_HOME/lib`），并且重启所有NodeManager。

# 分发Alluxio客户端jar包

我们需要让Alluxio客户端jar包对Hive可见，因为它里面包含配置好的`alluxio.hadoop.FileSystem`类。

可以通过以下方式进行配置：

- 将`alluxio-core-client-{{site.ALLUXIO_RELEASED_VERSION}}-jar-with-dependencies.jar`文件拷贝到Hive安装目录下的`lib`目录中。

#### 添加额外Alluxio配置到Hive中

如果你有其他需要对Hive指定的Alluxio配置属性，将它们添加到`hive-site.xml`中。例如，将`alluxio.user.file.writetype.default`
属性由默认的`MUST_CACHE`修改成`CACHE_THROUGH`：

```xml
<property>
<name>alluxio.user.file.writetype.default</name>
<value>CACHE_THROUGH</value>
</property>
```

# 在Alluxio上运行Hive

在Alluxio中为Hive创建相应目录：

```bash
$ ./bin/alluxio fs mkdir /tmp
$ ./bin/alluxio fs mkdir /user/hive/warehouse
$ ./bin/alluxio fs chmod 775 /tmp
$ ./bin/alluxio fs chmod 775 /user/hive/warehouse
```

接着你可以根据[Hive documentation](https://cwiki.apache.org/confluence/display/Hive/GettingStarted)来使用Hive了。

# Hive命令行示例

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

![HiveTableInAlluxio]({{site.data.img.screenshot_hive_table_in_alluxio}})

```
hive> select * from u_user;
```

你可以在命令行中看到相应查询结果：

![HiveQueryResult]({{site.data.img.screenshot_hive_query_result}})
