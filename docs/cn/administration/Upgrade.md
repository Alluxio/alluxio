---
layout: global
title: 升级
nickname: 升级
group: Administration
priority: 7
---

* Table of Contents
{:toc}

## 基础升级流程

正常情况下，用户可以直接关闭当前的 Alluxio 进程，将 Alluxio 二进制文件更改为更新的版本，同时还按之前的方式配置 Alluxio 集群，并使用现有的日志文件夹/地址来启动 Alluxio 进程
进行升级。Alluxio 可读取以前的日志文件并自动恢复 Alluxio 元数据。

以下两种情况下master日志无法向后兼容，需要采取额外的步骤来升级 Alluxio 集群：

- 从 Alluxio 1.x 版本升级到 Alluxio 2.x 版本
- 使用[内嵌日志]({{ '/cn/operation/Journal.html' | relativize_url}})的情况下从 Alluxio 2.3.x 及以下版本升级到 Alluxio 2.4.0 及以上版本

本文档介绍了如何将 Alluxio 升级到非向后兼容版本。 即使是要升级到可向后兼容的版本，仍然建议按照以下步骤在升级前创建备份。

## 创建当前版本的备份

Alluxio-1.8.1 版本引入了日志备份功能。
注意，请不要在备份前修改 Alluxio 二进制文件。
在运行 1.8.1 之前版本的 master 时，通过运行以下命令创建日志备份：

```console
$ ./bin/alluxio fsadmin backup
Successfully backed up journal to ${BACKUP_PATH}
```

`${BACKUP_PATH}` 将根据日志的日期和配置确定。
备份文件将默认保存到集群根 UFS 的 `alluxio.master.backup.directory` 中，
也可以使用 `backup [local_address] --local` 命令将文件备份到当前 leading master 节点的本地文件系统中。

## 升级并从备份启动

停止现有的 Alluxio 集群后，下载并解压新版本的 Alluxio 。
从 `/conf` 目录拷贝旧的配置文件。然后通过以下命令将集群格式化：

```console
$ ./bin/alluxio format
```
- **警告：** 该操作会对 Alluxio worker 上的内存虚拟硬盘进行格式化（即：删除其中的内容）。
如果您希望保留 worker 上的内存虚拟硬盘，请参阅
 [Alluxio worker 内存虚拟硬盘缓存持久化]({{ '/cn/administration/Upgrade.html' | relativize_url}}#alluxio-worker-ramdisk-cache-persistence)。

然后使用 `-i ${BACKUP_PATH}` 参数启动集群，
将 `${BACKUP_PATH}` 替换为具体的备份路径。

```console
$ ./bin/alluxio-start.sh -i ${BACKUP_PATH} all
```

注意这里的 `${BACKUP_PATH}` 应该是类似 HDFS 地址的完整路径，可以被所有 Alluxio master 访问。
如果要备份到本地文件系统路径，需将备份文件复制到所有 master 节点上的同一位置，然后通过本地备份文件路径启动所有 master。

## 升级客户端和服务器

Alluxio 2.x 版本对 RPC 层进行了重大修改，
因此 2.0.0 之前版本的客户端不能与 2.0.0 之后版本的服务器一起运行，反之亦然。
如果要使用 Alluxio-2.x 客户端需升级所有应用程序 。

请参阅以下步骤：
1. 备份 Alluxio 中文件的元数据。请参阅有关 `backup` 命令的[文档]({{ '/cn/operation/Admin-CLI.html' | relativize_url }}#backup)。
2. 停止 Alluxio 集群。
```console
$ ./bin/alluxio-stop.sh all
```
3. 更新所有应用程序的 Alluxio 客户端 jar 路径。例如`Yarn`, `Spark`, `Hive` 和 `Presto`，在 Cloudera Manager 的 “YARN (包括MR2)” 部分，在 “Configuration” 选项卡中，搜索参数 “Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh”。然后将以下行添加到脚本中：
```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```
	如下所示：
   ![locality]({{ '/img/screenshot_cdh_compute_hadoop_classpath.png' | relativize_url }})
4. 启动Alluxio集群
```console
$ ./bin/alluxio-start.sh all
```
5. 如果您已经更新了某个应用程序的 Alluxio 客户端 jar，请重新启动该应用程序，以便使用新的 Alluxio 客户端 jar。

## 其他选项

### Alluxio worker 内存磁盘缓存持久化

如果您已经配置了 Alluxio worker 上的内存磁盘缓存，可使用另一个存储介质（例如，主机的本地磁盘）来持久化和恢复这些缓存的内容。

在运行 `alluxio-stop.sh` 时加上 `-c` 参数来指定 worker 保存其内存磁盘内容的路径（worker会将内容保存到其主机的文件系统中）：
```
$ ./bin/alluxio-stop.sh workers -c ${CACHE_PATH}
```
- **警告：** 该操作将覆盖并替换给定 `${CACHE_PATH}` 中的所有现有内容

然后，在运行 `alluxio-start.sh` 时加上 `-c` 参数来指定包含 worker 内存磁盘缓存内容的目录。
```
$ ./bin/alluxio-start.sh workers NoMount -c ${CACHE_PATH}
```
- **警告：** 该操作将覆盖并替换已配置的 worker 内存磁盘路径中所有的现有内容。
