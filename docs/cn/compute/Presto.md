---
layout: global
title: Presto 使用 Alluxio
nickname: Presto
group: Compute Integrations
priority: 1
---

[Presto](https://prestosql.io/) 是一个开源的分布式 SQL 查询引擎，用于对数据进行大规模的交互式分析查询。
本指南介绍了如何使用 Alluxio 作为分布式缓存层运行 Presto 进行查询，其中数据源可以是 AWS S3、Azure Blob Store、HDFS 和许多其他数据源。
使用此设置，Alluxio 将帮助 Presto 访问数据（不论是何数据源），并透明地将频繁访问的数据（例如，常用的表）缓存到 Alluxio 的分布式存储中。
将 Alluxio worker 与 Presto worker 同置部署，可以提升数据本地性，减少 I/O 访问延迟，尤其是在数据是远程的或网络缓慢或拥塞的情况下效果更明显。

* Table of Contents
{:toc}

## 前期准备

* 安装 Java 8 Update 60 或更高版本（8u60+）的 64 位 Java。
* [部署 Presto](https://prestosql.io/docs/current/installation/deployment.html)。
  本指南基于`presto-0.208`测试。
* 已经安装并运行 Alluxio。
* 确保 Alluxio 客户端 jar 包是可用的。
  在从 Alluxio [下载页面](http://www.alluxio.io/download)下载的压缩包的`{{site.ALLUXIO_CLIENT_JAR_PATH}}`中，可以找到 Alluxio 客户端 jar 包。
* 确保 Hive metastore 正在运行以提供 Hive 表的元数据信息。

## 基础设置

### 配置 Presto 连接到 Hive Metastore

Presto 从 Hive Metastore 中获取数据库和表元数据的信息，同时获取表数据的文件系统位置。
编辑 Presto 的 `${PRESTO_HOME}/etc/catalog/hive.properties`配置：

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### 分发 Alluxio 客户端 jar 包给所有 Presto 服务器

把 Alluxio 客户端 jar 包`{{site.ALLUXIO_CLIENT_JAR_PATH}}` 放到所有 Presto 服务器的`${PRESTO_HOME}/plugin/hive-hadoop2/`目录（该目录可能会因版本而不同）中。重启 Presto 服务：

```console
$ ${PRESTO_HOME}/bin/launcher restart
```

在完成基础配置后，Presto 应该能够访问 Alluxio 中的数据。
要为 Presto 配置更高级的特性，请参考[高级设置](#advanced-setup)中的步骤。

## 示例：使用 Presto 在 Alluxio 上查询表

### 在 Alluxio 上创建 Hive 表

下面是一个依靠 Alluxio 中的文件在 Hive 中创建内部表的示例。
你可以从 [http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/) 下载数据文件（例如，`ml-100k.zip`）。
解压文件，并将文件`u.user`上传到 Alluxio 的`/ml-100k/`目录：

```console
$ ./bin/alluxio fs mkdir /ml-100k
$ ./bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio:///ml-100k
```

从 Alluxio 的已有文件创建一个 Hive 外部表：

```
hive> CREATE TABLE u_user (
userid INT,
age INT,
gender CHAR(1),
occupation STRING,
zipcode STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION 'alluxio://master_hostname:port/ml-100k';
```

在`http://master_hostname:19999`上查看 Alluxio WebUI，你可以看到 Hive 创建的目录和文件：

![HiveTableInAlluxio]({{ '/img/screenshot_presto_table_in_alluxio.png' | relativize_url }})

### 启动 Hive metastore

确保 Hive metastore 服务正在运行。Hive metastore 默认监听端口`9083`。如果 Hive metastore 没在运行，运行以下命令来启动 Hive metastore：

```console
$ ${HIVE_HOME}/bin/hive --service metastore
```

### 启动 Presto 服务器

启动 Presto 服务器。Presto 服务器默认在`8080`端口运行（可以通过`${PRESTO_HOME}/etc/config.properties`中的`http-server.http.port`设置）：

```console
$ ${PRESTO_HOME}/bin/launcher run
```

### 使用 Presto 查询表

按照 [Presto CLI 指南](https://prestosql.io/docs/current/installation/cli.html)下载`presto-cli-<PRESTO_VERSION>-executable.jar`，将其重命名为`presto`，并使用`chmod +x`使其变成可执行的（有时`${PRESTO_HOME}/bin/presto`中存在可执行的`presto`，你可以直接使用它）。

运行简单的查询（使用你实际的 Presto 服务器主机名和端口替换`localhost:8080`）：

```console
$ ./presto --server localhost:8080 --execute "use default;select * from u_user limit 10;" \
  --catalog hive --debug
```

你可以从控制台看到查询结果：

![PrestoQueryResult]({{ '/img/screenshot_presto_query_result.png' | relativize_url }})

Presto 服务器日志：

![PrestoQueryLog]({{ '/img/screenshot_presto_query_log.png' | relativize_url }})

## 高级设置

### 自定义 Alluxio 用户属性

要配置其他 Alluxio 属性，可以将包含[`alluxio-site.properties`]({{ '/cn/operation/Configuration.html' | relativize_url }})的配置路径（即`${ALLUXIO_HOME}/conf`）追加到 Presto 文件夹下的`etc/jvm.config`的 JVM 配置中。
这种方法的优点是能够在同一个`alluxio-site.properties`文件中设置所有的 Alluxio 属性。

```bash
...
-Xbootclasspath/a:<path-to-alluxio-conf>
```

或者，你可以将这些属性添加到 Hadoop 配置文件（`core-site.xml`，`hdfs-site.xml`）中，并使用文件`$presto/etc/catalog/hive.properties`中的 Presto 属性`hive.config.resources`来为每个 Presto worker 指明文件位置。

```
hive.config.resources=/<PATH_TO_CONF>/core-site.xml,/<PATH_TO_CONF>/hdfs-site.xml
```

#### 示例：连接 HA 模式的 Alluxio

要使用容错模式的 Alluxio，需要在 classpath 中的`alluxio-site.properties`文件中适当设置 Alluxio 集群的属性。

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=zkHost1:2181,zkHost2:2181,zkHost3:2181
```

或者，你可以将属性添加到`hive.config.resources`所包含的 hadoop `core-site.xml`配置中。

```xml
<configuration>
  <property>
    <name>alluxio.zookeeper.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>alluxio.zookeeper.address</name>
    <value>zkHost1:2181,zkHost2:2181,zkHost3:2181</value>
  </property>
</configuration>
```

#### 示例：更改 Alluxio 默认写类型

例如，更改`alluxio.user.file.writetype.default`，从默认的`MUST_CACHE`改为`CACHE_THROUGH`。

一种方法是在`alluxio-site.properties`中设置属性，并将此文件分发到每个 Hive 节点的 classpath：

```properties
alluxio.user.file.writetype.default=CACHE_THROUGH
```

或者，更改`conf/hive-site.xml`：

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

### 提高并行度

Presto 的 Hive 集成使用[`hive.max-split-size`](https://teradata.github.io/presto/docs/141t/connector/hive.html)配置来控制查询的并行度。
对于 Alluxio 1.6 或更早版本，建议将此大小设置为不小于 Alluxio 的块大小，以避免同一块中的读取竞争。对于以后的 Alluxio 版本，由于 Alluxio Worker 上有了异步缓存，这不再是一个问题。

### 避免 Presto 读取大文件超时

 建议将`alluxio.user.network.data.timeout`增加到较大的值（例如`10min`），以避免从远程的 worker 读取大文件时超时失败。

## 故障排除指南

### 查询出现错误信息“No FileSystem for scheme: alluxio”

当你看到类似如下错误信息时，很可能 Alluxio 客户端 jar 包没有被放入到 Presto worker 的 classpath 中。请按照[说明](#distribute-the-alluxio-client-jar-to-all-presto-servers)来解决此问题。

```
Query 20180907_063430_00001_cm7xe failed: No FileSystem for scheme: alluxio
com.facebook.presto.spi.PrestoException: No FileSystem for scheme: alluxio
	at com.facebook.presto.hive.BackgroundHiveSplitLoader$HiveSplitLoaderTask.process(BackgroundHiveSplitLoader.java:189)
	at com.facebook.presto.hive.util.ResumableTasks.safeProcessTask(ResumableTasks.java:47)
	at com.facebook.presto.hive.util.ResumableTasks.access$000(ResumableTasks.java:20)
	at com.facebook.presto.hive.util.ResumableTasks$1.run(ResumableTasks.java:35)
	at io.airlift.concurrent.BoundedExecutor.drainQueue(BoundedExecutor.java:78)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```
