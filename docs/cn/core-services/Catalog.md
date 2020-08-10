---
layout: global
title: Catalog
nickname: Catalog
group: Core Services
priority: 2
---

*Table of Contents
{:toc}

## 概述

Alluxio 2.1.0引入了一项Alluxio目录服务的新Alluxio服务。
Alluxio目录是用于管理对结构化数据访问的服务，
所提供服务类似于[Apache Hive Metastore](https://cwiki.apache.org/confluence/display/Hive/Design#Design-Metastore)

SQL Presto，SparkSQL和Hive等引擎利用这些类似于元数据存储的服务来确定执行查询时要读取哪些数据以及读取多少数据。Catalog中存储有关不同数据库目录，表，存储格式，数据位置等及更多信息。
Alluxio目录服务旨在使为Presto查询引擎提供结构化表元数据检索和服务变得更简单和直接，例如[PrestoSQL](https://prestosql.io/)，
[PrestoDB](https://prestodb.io)和[Starburst Presto](https://starburstdata.com)。

## 系统架构

Alluxio目录服务的设计与普通的Alluxio文件系统非常相似。
服务本身并不负责保有所有数据，而是作为一个源自另一个位置(如MySQL，Hive)的
元数据缓存服务。
这些被称为**UDB**(**U**nder **D**ata**B**ase)。
UDB负责元数据的管理和存储。
当前，Hive是唯一支持的UDB。
Alluxio目录服务通过Alluxio文件系统命名空间来缓存和使元数据全局可用。

```
Query Engine     Metadata service                   Under meta service
+--------+       +--------------------------+       +----------------+
| Presto | <---> | Alluxio Catalog Service  | <---> | Hive Metastore |
+--------+       +--------------------------+       +----------------+
```

当用户表跨多个存储服务(即AWS S3，HDFS，GCS)时，
为了做查询和读取等请求，通常用户需要配置其SQL引擎分别逐一连接到每个存储服务，
使用了Alluxio目录服务之后，用户只需配置一个Alluxio客户端，通过Alluxio就可以读取和查询任何支持的底层存储系统及数据。

## 使用Alluxio目录服务

以下是Alluxio目录服务的基本配置参数以及与Alluxio目录服务交互的方式。更多详细信息
可以在[命令行界面文档]中找到({{ '/en/operation/User-CLI.html' | relativize_url}}＃table-operations)。

### Alluxio服务器配置

默认情况下，目录服务是启用的。要明确停止目录服务，添加以下配置行到 alluxio-site.properties

```properties
alluxio.table.enabled=false
```

默认情况下，挂载的数据库和表将在Alluxio`/catalog`目录下。
可以通过以下配置为目录服务配置非默认根目录，

```properties
alluxio.table.catalog.path=</desired/alluxio/path>
```

### 附加数据库

为了让Alluxio能提供有关结构化表数据信息服务，必须告知Alluxio
有关数据库的位置。
使用Table CLI来执行任何与从Alluxio附加，浏览或分离数据库相关操作。

```console
$ ${ALLUXIO_HOME}/bin/alluxio table
Usage: alluxio table [generic options]
	 [attachdb [-o|--option <key=value>] [--db <alluxio db name>] [--ignore-sync-errors] <udb type> <udb connection uri> <udb db name>]
	 [detachdb <db name>]
	 [ls [<db name> [<table name>]]]
	 [sync <db name>]
	 [transform <db name> <table name>]
	 [transformStatus [<job ID>]]
```

要附加数据库，使用`attachdb`命令。目前，hive和glue是支持的
`<udb type>`。
有关更多详细信息参见[attachdb命令文档]({{ '/en/operation/User-CLI.html' | relativize_url}}＃attachdb)。
以下命令从位于`Thrift://metastore_host:9083`的元数据存储将hive数据库`default`映射到Alluxio中名为`alluxio_db`的数据库

```console
$ ${ALLUXIO_HOME}/bin/alluxio table attachdb --db alluxio_db hive \
    thrift://metastore_host:9083 default
```

> **注意:**附加数据库后，所有表都从已配置的UDB同步过了。
如果数据库或表发生带外更新，并且用户希望查询结果反映这些
更新，则必须同步数据库。有关更多信息参见[同步数据库](＃syncing-databases)。

### 探索附加的数据库

数据库一旦附加，通过`alluxio table ls`检查已挂载数据库

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls
alluxio_db
```

通过`alluxio tables ls <db_name>`列出数据库下所有表。
相应hive数据库中存在的所有表都会通过此命令列出

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls alluxio_db
test
```

在上述情况下，`alluxio_db`有一个表`test`。要获取数据库中表的更多信息，运行
```console
$ alluxio table ls <db_name> <table_name>
```
此命令会将表的信息通过控制台输出。输出示例如下:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table ls alluxio_db test
db_name: "alluxio_db"
table_name: "test"
owner: "alluxio"
schema {
  cols {
    name: "c1"
    type: "int"
    comment: ""
  }
}
layout {
  layoutType: "hive"
  layoutSpec {
    spec: "test"
  }
  layoutData: "\022\004test\032\004test\"\004test*\345\001\n\232\001\n2org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\022(org.apache.hadoop.mapred.TextInputFormat\032:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\0227alluxio://localhost:19998/catalog/test/tables/test/hive\032\v\020\377\377\377\377\377\377\377\377\377\001 \0002\v\022\002c1\032\003int*\000"
}
parameters {
  key: "totalSize"
  value: "6"
}
parameters {
  key: "numRows"
  value: "3"
}
parameters {
  key: "rawDataSize"
  value: "3"
}
parameters {
  key: "COLUMN_STATS_ACCURATE"
  value: "{\"BASIC_STATS\":\"true\"}"
}
parameters {
  key: "numFiles"
  value: "3"
}
parameters {
  key: "transient_lastDdlTime"
  value: "1571191595"
}
```

### 分离数据库

如果需要分离数据库，可以从Alluxio命名空间用以下命令

```console
$ alluxio table detach <database name>
```

针对前面的例子，分离数据库可以运行:

```console
$ ${ALLUXIO_HOME}/bin/alluxio table detachdb alluxio_db
```

运行上述命令之后再运行`alluxio table ls`将不会再显示已分离的数据库。

### 同步数据库

当原始底层数据库和表更新后，用户可以调用sync命令
来刷新存储在Alluxio目录元数据中的相应信息。
有关更多详细信息参见[sync命令文档]({{ '/en/operation/User-CLI.html' | relativize_url}}＃sync)。

```console
$ alluxio table sync <database name>
```

针对前面的例子，可以运行以下命令来sync数据

```console
$ ${ALLUXIO_HOME}/bin/alluxio table sync alluxio_db
```

上述sync命令将根据UDB表的更新，删除，和添加等来刷新Alluxio相应目录元数据。

## 通过Presto使用Alluxio结构化数据

PrestoSQL版本332或更高版本以及PrestoDB版本0.232或更高版本在其hive-hadoop2连接器中内置了对Alluxio目录服务的支持。
有关使用PrestoSQL或PrestoDB相应版本设置Alluxio目录服务的说明，
请参阅PrestoSQL文档[documentation](https://prestosql.io/docs/current/connector/hive.html#alluxio-configuration) 
或PrestoDB文档[documentation](https://prestodb.io/docs/current/connector/hive.html#alluxio-configuration)。

如果使用的是PrestoSQL或PrestoDB的早期版本，则可以使用alluxio发行版中包含的hive-alluxio连接器。
最新的Alluxio发行版包含一个presto连接器jar文件，可以通过将其放入
`$ {PRESTO_HOME}/plugins`目录中激活经由Presto到目录服务的连接。

### 在Presto中启用Alluxio目录服务

假设已经分别在本地计算机上的$`${ALLUXIO_HOME}` 和`${PRESTO_HOME}` 上安装了Alluxio和Presto
，要启用Alluxio目录服务需要在所有Presto节点上将Alluxio连接器文件作为一个新的插件复制到
Presto安装中。 

对于PrestoSQL安装，运行以下命令。 

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/presto-hive-alluxio-319/ ${PRESTO_HOME}/plugin/hive-alluxio/
```

对于PrestoDB安装，运行以下命令。 

```console
$ cp -R ${ALLUXIO_HOME}/client/presto/plugins/prestodb-hive-alluxio-227/ ${PRESTO_HOME}/plugin/hive-alluxio/
```

另外，你还需要创建一个新的目录来使用Alluxio连接器和
Alluxio目录服务

`/etc/catalog/catalog_alluxio.properties`
```properties
connector.name=hive-alluxio
hive.metastore=alluxio
hive.metastore.alluxio.master.address=HOSTNAME:PORT
```

创建`catalog_alluxio.properties`文件，意味着一个名为`catalog_alluxio`新的目录
添加到Presto。
设置`connector.name=hive-alluxio`会将连接器类型设置为Presto新Alluxio连接器名称，即`hive-alluxio`。
如果使用的是PrestoSQL版本332或更高版本以及PrestoDB版本0.232或更高版本，hive-hadoop2连接器中内置了对Alluxio Catalog Service的支持，因此应设置`connector.name=hive-hadoop2`。
`hive.metastore=alluxio`表示Hive元数据存储连接将使用`alluxio`类型与Alluxio目录服务进行通信。
设置`hive.metastore.alluxio.master.address=HOSTNAME:PORT`定义了
Alluxio目录服务的主机和端口，与Alluxio master主机和端口相同。
一旦在每个节点上配置后，重新启动所有presto coordinators和workers。

###在Presto中使用Alluxio目录服务

为了使用Alluxio Presto插件，使用以下命令启动presto CLI(假设
`/etc/catalog/catalog_alluxio.properties`文件已创建)

```console
$ presto --catalog catalog_alluxio
```

默认情况下，presto执行的任何查询都会通过Alluxio的目录服务来获取数据库和表信息。

通过运行以下一些查询来确认配置正确:

- 列出附加的数据库:

```sql
SHOW SCHEMAS;
```

- 列出一种数据库模式下的表:

```sql
SHOW TABLES FROM <schema name>;
```

- 运行一个简单的查询，该查询将从元数据存储中读取数据并从表中加载数据:

```sql
DESCRIBE <schema name>.<table name>;
SELECT count(*) FROM <schema name>.<table name>;
```

