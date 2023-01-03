---
layout: global
title: 通过 Alluxio 使用 Presto 查询 Iceberg 表
nickname: Presto on Iceberg (Experimental)
group: Compute Integrations
priority: 2
---

Presto 在 0.256 版本中新增了对 [Iceberg 表](https://iceberg.apache.org/)支持的功能。

本文档介绍如何通过 Alluxio 使用 Presto 查询 Iceberg 表。本文档目前处于实验性阶段，此处所述信息可能会发生变化。
* Table of Contents
{:toc}

要使用 Presto 查询 Iceberg 表，请确保已设置好 Presto，Hive Metastore 和 Alluxio，并且 Presto 可以通过 Alluxio 的文件系统接口访问数据。否则，请参考 Presto 的通用安装和配置[指南]({{ '/cn/compute/Presto.html' | relativize_url }})进行设置。该指南的大部分内容也适用于 Iceberg 流程，本文档包含了使用 Iceberg 表的详细说明。
## 部署条件

* Presto 通用设置的所有[部署条件]({{ '/cn/compute/Presto.html' | relativize_url }}#部署条件)
* 服务器版本不低于 0.257

## 基本设置

### 在 Presto 连接器上安装 Alluxio client jar

将位于 `{{site.ALLUXIO_CLIENT_JAR_PATH}}` 的 Alluxio client jar 复制到位于  `${PRESTO_HOME}/plugin/iceberg/` 的 Presto Iceberg 连接器路径中， 然后重启 Presto 服务器：

```console
$ ${PRESTO_HOME}/bin/launcher restart
```

这里需注意，相同的 client jar 文件应位于 Hive 的类路径下。否则，请参阅共同部署 Hive 和 Alluxio 的[章节]({{ '/cn/compute/Hive.html' | relativize_url }}#基本设置)来设置 Hive。

### 配置 Presto 来使用 Iceberg 连接器

Presto 使用 [Iceberg 连接器](https://prestodb.io/docs/current/connector/iceberg.html)读取和写入 Iceberg 表。要启用 Iceberg 连接器，在 Presto 的安装目录 `${PRESTO_HOME}/etc/catalog/iceberg.properties` 中为 Iceberg 连接器创建一个 catalog：

```properties
connector.name=iceberg
hive.metastore.uri=thrift://localhost:9083
```

根据设置，修改 Hive Metastore 的连接 URI。
## 示例：使用 Presto 查询 Alluxio 上的Iceberg表

### 创建 schema 和 Iceberg 表

为了方便演示，我们创建一个示例 schema 和一个 Iceberg 表。
使用以下命令启动 Presto CLI client：

```console
./presto --server localhost:8080 --catalog iceberg --debug
```

有关 client 的更多信息，请参阅[使用 Presto 查询表]({{ '/cn/compute/Presto.html' | relativize_url }}#query-tables-using-presto)章节。由于我们要处理的是 Iceberg 表，这里需将 catalog 设置为`iceberg` 。
在 client 运行以下语句：

```sql
CREATE SCHEMA iceberg_test;
USE iceberg_test;
CREATE TABLE person (name varchar, age int, id int)
    WITH (location = 'alluxio://localhost:19998/person', format = 'parquet');
```

更改 Alluxio 连接器 URI 中的 hostname 和端口来匹配你的配置。

这些语句在 Alluxio 文件系统的 `/person` 路径下创建一个名为 `iceberg_test` 的 schema 和一个名为 `person` 的表，并将表存成 Parquet 格式。

### 在表中插入示例数据

在新创建的表中插入一行示例数据:

```sql
INSERT INTO person VALUES ('alice', 18, 1000);
```

注意：Presto 中 Iceberg 连接器的写入路径存在 bug，因此数据插入可能会失败。在 Presto 0.257 版本中，[此 PR](https://github.com/prestodb/presto/pull/16275) 已解决该问题。

也可查看Alluxio中的文件现在可通过从表中读回数据来验证是否一切正常:

```sql
SELECT * FROM person;
```

也可查看 Alluxio 中的文件:

```console
$ bin/alluxio fs ls /person
drwxr-xr-x  alluxio    alluxio    10    PERSISTED 06-29-2021 16:24:02:007  DIR /person/metadata
drwxr-xr-x  alluxio    alluxio     1    PERSISTED 06-29-2021 16:24:00:049  DIR /person/data
$ bin/alluxio fs ls /person/data
-rw-r--r--  alluxio    alluxio   400    PERSISTED 06-29-2021 16:24:00:691 100% /person/data/6e6a451a-8f20-4d73-9ef6-ee48070dad27.parquet
$ bin/alluxio fs ls /person/metadata
-rw-r--r--  alluxio    alluxio  1406    PERSISTED 06-29-2021 16:23:28:608 100% /person/metadata/00000-2fd982ae-2a81-44a8-a4db-505e9ba6c09d.metadata.json
...
(snip)
```

这里可以看到, Iceberg 表的 metadata 文件与 data 文件已经创建成功。
