---
layout: global
title: 数据转换
nickname: 数据转换
group: Core Services
priority: 4
---

* Table of Contents
{:toc}

## 数据转换

通过作业服务和目录服务，Alluxio可以将一个表转换为新表。如果表未分区，则转换是在表级运行的。如果表已分区，则转换将在分区级运行。原始表的数据是不会被修改的，新表的数据将在Alluxio管理的新存储位置永久保留。一旦转换完成后，Presto用户可以透明地查询新数据。

目前有两种支持的转换类型:

1.合并文件，以便每个文件都至少达到一定的大小，并且最多不超过一定数量的文件。
2.将CSV文件转换为Parquet文件

>在Alluxio版本2.3.0中，转换后的数据将总写为Parquet格式。

在运行转换之前，首先要attach一个数据库。以下命令将Hive中的“默认”数据库attach到Alluxio。

```console
$ ${ALLUXIO_HOME}/bin/alluxio table attachdb hive thrift://localhost:9083 default
```

转换是通过命令行界面调用的。以下命令将表`test`的每个分区下的文件合并为最多100个文件。
可在[command line interface documentation]({{ '/en/operation/User-CLI.html' | relativize_url}}＃table-operations)找到有关transform命令的其他详细信息。

```console
$ ${ALLUXIO_HOME}/bin/alluxio table transform default test
```

运行上面的命令后，你会看到如下输出

```console
Started transformation job with job ID 1572296710137, you can monitor the status of the job with './bin/alluxio table transformStatus 1572296710137'.
```
 
现在，按输出中的指令来监控数据变换的状态

```console
$ ${ALLUXIO_HOME}/bin/alluxio table transformStatus 1572296710137
```

上述命令将显示转换作业的状态

```console
database: default
table: test
transformation: write(hive).option(hive.file.count.max, 100).option(hive.file.size.min, 2147483648)
job ID: 1572296710137
job status: COMPLETED
```

因转换成功完成，现在可以直接在转换后的表上透明地运行Presto查询。

可以使用以下Presto查询找到转换后的数据的位置:

```console
presto:default> select "$path" from test;
```

应看到类似如下输出:

```console
alluxio://localhost:19998/catalog/default/tables/test/_internal_/part=0/20191024-213102-905-R29wf/part-0.parquet
```

