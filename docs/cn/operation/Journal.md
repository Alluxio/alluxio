---
layout: global
title: 日志
nickname: 日志
group: Operations
priority: 4
---

* Table of Contents
{:toc}

Alluxio维护日志，以支持元数据操作的持久性。当请求修改Alluxio状态时，例如创建或重命名文件在返回之前，
Alluxio将为操作写一个日志条目对客户的成功回应。日记条目是写向持久存储，如磁盘或HDFS，所以即使是Alluxio master进程被终止，
状态将在重新启动时恢复。

# 配置

要为日志设置的最重要的配置值是`alluxio.master.journal.folder`。这必须设置为所有主服务器都可以使用的共享文件系统。
在单主节店模式下，直接使用本地文件系统路径是可行的。对于分布在不同机器上的多个主目录，共享文件夹应该位于支持flush的分布式系统中，
比如HDFS或NFS。不建议将日志放在对象存储中。对于对象存储，对日志的每一次更新都需要创建一个新对象，
这对于大多数紧急的用例来说是非常缓慢的。

**配置示例:**
使用HDFS来存储日志：
```
alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/dir/alluxio_journal
```
使用本地文件系统来存储日志：
```
alluxio.master.journal.folder=/opt/alluxio/journal
```

# 格式化

第一次启动Alluxio master节点时，日志必须格式化。

**警告: 格式化日志将会删除Alluxio所有元数据**
```console
$ ./bin/alluxio formatMaster
```

# 备份

Alluxio支持对日志进行备份，以便可以将Alluxio元数据恢复到以前的时间点。
生成备份会在备份发生时导致服务临时不可用。

使用 `fsadmin backup`命令生成备份。
```console
$ ./bin/alluxio fsadmin backup
```

默认情况下，这将编写一个名为`alluxio-journal-YYYY-MM-DD-timestamp.gz`的备份指向文件系统下根目录的"/alluxio_backups"目录，
例如:hdfs://cluster/alluxio_backups。这个默认的备份目录可以通过设置`alluxio.master.backup.directory`来配置。

```
alluxio.master.backup.directory=/alluxio/backups
```

查看 [备份指令]({{ '/cn/operation/Admin-CLI.html' | relativize_url }}#backup) 获取写备份文件具体位置的额外配置。

# 恢复

要从日志备份中恢复Alluxio系统，请停止系统，格式化，
然后重新启动系统，使用 `-i`(import) 标志传递备份的URI。

```console
$ ./bin/alluxio-stop.sh masters
$ ./bin/alluxio formatMaster
$ ./bin/alluxio-start.sh -i <backup_uri> masters
```

 `<backup_uri>` 应该是对所有主机都可用的完整URI路径, e.g.
`hdfs://[namenodeserver]:[namenodeport]/alluxio_backups/alluxio-journal-YYYY-MM-DD-timestamp.gz`

如果恢复成功，您应该会在master节点主日志中看到一行日志消息
```
INFO AlluxioMasterProcess - Restored 57 entries from backup
```
