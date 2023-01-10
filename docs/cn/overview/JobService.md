---
layout: global
title: 作业服务器
group: Overview
priority: 3
---

* 内容列表
{:toc}

## 架构概览

Alluxio 作业服务器是负责将各种不同类型的操作分配给Job Worker的任务调度框架。

Master负责将作业分配为更小的任务，供Job Worker执行并管理作业的完成状态。

Job Worker将来自Job Master的任务排列（queue)，并通过管理可配置的固定线程池(`alluxio.job.worker.threadpool.size`)来完成这些任务。

## 不同类型的作业

### 加载 Load

`fs distributedLoad`CLI命令中使用了加载作业，按特定的副本数将文件加载到Alluxio。

### 迁移 Migrate

`fs distributedCp`和`fs distributedMv`CLI命令中使用了迁移作业，使用固定的[写入类型]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }})进行数据复制/移动。

### 持久化 Persist

`fs persist` CLI命令间接使用了持久化作业，以`ASYNC_THROUGH`[写入类型]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }})写入Alluxio时使用持久化作业在后台进行持久化。

该作业负责将Alluxio中的文件持久化到特定的ufs路径中。

### 驱逐 Evict

`fs free` CLI命令和后台复制进程间接使用了驱逐作业。

该作业负责从Alluxio中驱逐出特定数量的数据块副本。

### 移动 Move

复制后台进程使用移动作业将数据块从一个worker移动到另一个worker。

### 复制 Replicate

后台复制进程使用复制作业将数据块从一个worker复制到特定数量的其他worker上。

## 巡检命令

作业服务器提供以下一系列的巡检命令。

### fsadmin report jobservice

`fsadmin report jobservice` 会报告作业服务器摘要。

```console
$ ./bin/alluxio fsadmin report jobservice
Worker: MigrationTest-workers-2  Task Pool Size: 10     Unfinished Tasks: 1303   Active Tasks: 10     Load Avg: 1.08, 0.64, 0.27
Worker: MigrationTest-workers-3  Task Pool Size: 10     Unfinished Tasks: 1766   Active Tasks: 10     Load Avg: 1.02, 0.48, 0.21
Worker: MigrationTest-workers-1  Task Pool Size: 10     Unfinished Tasks: 1808   Active Tasks: 10     Load Avg: 0.73, 0.5, 0.23

Status: CREATED   Count: 4877
Status: CANCELED  Count: 0
Status: FAILED    Count: 1
Status: RUNNING   Count: 0
Status: COMPLETED Count: 8124

10 Most Recently Modified Jobs:
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371976       Name: Persist             Status: COMPLETED
Timestamp: 10-28-2020 22:02:34:001       Id: 1603922371982       Name: Persist             Status: COMPLETED
(only a subset of the results is shown)

10 Most Recently Failed Jobs:
Timestamp: 10-24-2019 17:15:22:946       Id: 1603922372008       Name: Persist             Status: FAILED

10 Longest Running Jobs:
```

### job ls

`job ls` 会列出正在作业服务器上运行或运行过的作业。

```console
$ ./bin/alluxio job ls
1613673433925   Persist    COMPLETED
1613673433926   Persist    COMPLETED
1613673433927   Persist    COMPLETED
1613673433928   Persist    COMPLETED
1613673433929   Persist    COMPLETED
```

### job stat -v <job_id> 

`job stat -v <job_id>` 会列出某个作业的详细信息。（加 `-v` 表示包含worker上指定任务的信息）

```console
bin/alluxio job stat -v 1613673433929
ID: 1613673433929
Name: Persist
Description: PersistConfig{filePath=/test5/lib/alluxio-underfs-cosn-2.5.0-SNAPSHOT.jar, mountId=1, overwrite=false, ufsPath=...
Status: COMPLETED
Task 0
	Worker: 192.168.42.71
	Status: COMPLETED
```

