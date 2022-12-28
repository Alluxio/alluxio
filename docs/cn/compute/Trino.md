---
layout: global
title: Running Trino with Alluxio
nickname: Trino
group: Compute Integrations
priority: 2
---

[Trino](https://trino.io/)
是一个开源的分布式 SQL 查询引擎，可以在大规模数据上运行交互式分析查询。
本指南介绍了如何使用 Alluxio 作为分布式缓存层在 Trino 上对 Alluxio 支持的任何数据存储系统（如 AWS S3、HDFS、Azure Blob Store、NFS 等）进行查询。
Alluxio 允许 Trino 从各种数据源获取数据，并将经常访问的数据（例如常用表）透明地缓存到 Alluxio 分布式存储中。
将 Alluxio worker 与 Trino worker 部署在一起可以提高数据本地性，并在其他存储系统远程或网络缓慢或阻塞时减少 I/O 访问延迟。

* Table of Contents
{:toc}

## 前置条件

* Java 配置为 Java 11，版本不低于 11.0.7，64 位，与 Trino 要求一致
* Python 版本为 2.6.x, 2.7.x, 或者 3.x, 与 Trino 要求一致
* [部署 Trino](https://trino.io/docs/current/installation/deployment.html)
这篇指南用 `Trino-352` 进行测试
* Alluxio 已经被配置好而且开始运行
* 确保 Alluxio client jar 可用
  这个 Alluxio 客户端 jar 文件可以在从 Alluxio [下载页面](https://www.alluxio.io/download)下载的 tarball 中的 {{site.ALLUXIO_CLIENT_JAR_PATH}} 处找到
* 请确保 Hive MetaStore 正在运行以提供 Hive table 的元数据信息

## 基本配置

### 配置 Trino 与 Hive Metastore

Trino 通过 Trino 的 Hive connector从 Hive 元数据仓库获取数据库和表元数据信息（包括文件系统位置）。
这里是一个 catalog 使用 Hive connector 的 Trino 配置文件示例 `${Trino_HOME}/etc/catalog/hive.properties`，其中 Metastore 位于本地主机上

```properties
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
```

### 将 Alluxio client  jar 分发到所有 Trino 服务器上

为了使 Trino 能够与 Alluxio 服务器通信，必须将 Alluxio client jar 放在 Trino 服务器的 classpath 中。
将 Alluxio client jar `{{site.ALLUXIO_CLIENT_JAR_PATH}}` 放到所有 Trino 服务器的路径
`${Trino_HOME}/plugin/hive-hadoop2/` 下
(此目录在不同版本中可能有所不同)。重启 Trino worker 和
coordinator:

```console
$ ${Trino_HOME}/bin/launcher restart
```

在完成基本配置后，Trino 应该能够访问 Alluxio 中的数据。
要为 Trino 配置更高级的功能（例如，使用 HA 连接 Alluxio），请按照[高级设置](#高级设置)中的说明进行操作。

## 示例：使用 Trino 查询 Alluxio 上的表

### 在 Alluxio 上创建 Hive table

下面是一个在Hive中创建一个由Alluxio中的文件支持的内部表的例子。
你可以从 [http://grouplens.org/datasets/movielens/](http://grouplens.org/datasets/movielens/) 下载数据文件 (e.g. `ml-100k.zip`)。
解压该文件然后将 `u.user` 上传至 Alluxio 中的 `/ml-100k/`:

```console
$ ./bin/alluxio fs mkdir /ml-100k
$ ./bin/alluxio fs copyFromLocal /path/to/ml-100k/u.user alluxio:///ml-100k
```

创建指向 Alluxio 文件位置的外部 Hive table。

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

您可以通过访问 Alluxio WebUI `http://master_hostname:19999` 查看 Hive 创建的目录和文件。

### 启动Hive Metastore

确保您的 Hive Metastore 服务正在运行。Hive 元数据仓库默认情况下在端口 `9083` 上运行。如果未运行，请执行以下命令启动 Metastore：

```console
$ ${HIVE_HOME}/bin/hive --service metastore
```

### 启动 Trino 服务器

启动你的 Trino 服务器。Trino 服务器默认情况下在端口 8080上运行 (在  `${Trino_HOME}/etc/config.properties`  中的 `http-server.http.port` 设置):

```console
$ ${Trino_HOME}/bin/launcher run
```

### 用 Trino 查询表格

按照 [Trino CLI 说明](https://trino.io/docs/current/installation/cli.html)下载 `trino-cli-<Trino_VERSION>-executable.jar`，将其重命名为 `trino`，并使用 `chmod +x` 命令使其可执行（有时可执行文件 `trino` 存在于 `${trino_HOME}/bin/trino` 中，您可以直接使用）。

运行单个查询（将`localhost:8080` 替换为实际的 Trino 服务器主机名和端口）：

```console
$ ./trino --server localhost:8080 --execute "use default; select * from u_user limit 10;" \
  --catalog hive --debug
```

## 高级设置

### 定制化 Alluxio 用户属性

要配置其他 Alluxio 属性，您可以将包含 [`alluxio-site.properties`]({{ '/en/operation/Configuration.html' | relativize_url }}) 的配置路径（即  `${ALLUXIO_HOME}/conf`）附加到 Trino 文件夹下的 `etc/jvm.config` 中 Trino 的 JVM 配置里。此方法的优点是在 `alluxio-site.properties` 的同一文件中设置所有 Alluxio 属性。

```bash
...
-Xbootclasspath/a:<path-to-alluxio-conf>
```

或者，将 Alluxio 配置项添加到 Hadoop 配置文件（(`core-site.xml`，`hdfs-site.xml`）中，并在文件 `${Trino_HOME}/etc/catalog/hive.properties` 中将每一个 Trino worker的属性 `hive.config.resources` 指向 Hadoop 资源的位置。

```
hive.config.resources=/<PATH_TO_CONF>/core-site.xml,/<PATH_TO_CONF>/hdfs-site.xml
```

#### 示例: 连接高可用模式（HA）的 Alluxio 集群

如果 Alluxio HA 集群使用Embedded Journal模式的高可用，请在 classpath 上的 `alluxio-site.properties` 文件中适当设置 Alluxio 集群属性。

```properties
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

或者，您可以将属性添加到 `hive.config.resources` 包含的 Hadoop `core-site.xml` 配置中。

```xml
<configuration>
  <property>
    <name>alluxio.master.rpc.addresses</name>
    <value>master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998</value>
  </property>
</configuration>
```

有关如何连接使用基于 ZooKeeper（UFS Journal 模式）的 Alluxio 高可用集群，请参阅[高可用模式客户端配置参数]({{ '/cn/deploy/Running-Alluxio-On-a-HA-Cluster.html' | relativize_url }}#specify-alluxio-service-in-configuration-parameters)。

#### 示例：更改 Alluxio 默认写入类型

例如，将 `alluxio.user.file.writetype.default` 从默认值  `ASYNC_THROUGH` 更改为 `CACHE_THROUGH`。

可以在 `alluxio-site.properties` 中指定该属性，并将此文件分发到每个 Trino 节点的 classpath：

```properties
alluxio.user.file.writetype.default=CACHE_THROUGH
```

或者，修改 `conf/hive-site.xml` 以包括：

```xml
<property>
  <name>alluxio.user.file.writetype.default</name>
  <value>CACHE_THROUGH</value>
</property>
```

### 增加并行度

Trino 的 Hive connector 使用配置 `hive.max-split-size` 来控制查询的并行性。 对于 Alluxio 1.6 或更早版本，建议将此大小设置为不小于 Alluxio 的块大小，以避免在同一块内的读冲突。 在 Alluxio 的后续版本中考虑到 Alluxio 的异步缓存能力，此问题不再存在。

### 避免 Trino 在读取大文件时超时

建议将  `alluxio.user.streaming.data.timeout` 的值增大（e.g. 10min），以避免从远端 worker 读取大文件时出现超时失败。
