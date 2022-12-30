---
layout: global
title: 作业服务
group: Overview
priority: 3
---

* 内容列表
{:toc}

## 架构概览

Alluxio 作业服务是负责将各种不同类型的操作分配给 Job Worker的任务调度框架。

Master 负责将Job分配为更小的任务，供 Job Worker 执行并管理Job的完成状态。

Job Worker 将来自 Job Master 的任务排列（queue)，并通过管理可配置的固定线程池(`alluxio.job.worker.threadpool.size`)来完成这些任务。

## 不同类型的作业

### 加载

Load Job 作为`fs distributedLoad` CLI 命令的一部分，会按特定的副本数将文件加载到 Alluxio。

### 迁移

Migrate Job 作为`fs distributedCp`  和 `fs distributedMv` CLI 命令的一部分，负责使用固定的 [WriteType]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }}) 进行数据复制/移动。

### 持久化

Persist Job 间接作为`fs persist` CLI 命令的一部分，在使用 ASYNC_THROUGH [WriteType]({{ '/en/overview/Architecture.html#data-flow-write' | relativize_url }}) 写入 Alluxio 时在后台进行持久化。

该Job负责将 Alluxio 中的文件持久化到特定的 ufs 路径中。

### 驱逐

Evict Job 由 `fs free` CLI 命令和后台复制进程间接使用。

该Job负责从 Alluxio 中驱逐出特定数量的数据块副本。

### 移动

复制后台进程使用 Move Job 将数据块从一个 worker 移动到另一个 worker。

### 复制

后台复制进程使用Replicate Job将数据块从一个worker复制到特定数量的其他worker上。

## 巡检命令

作业服务提供大量的巡检命令。

### fsadmin report jobservice

`fsadmin report jobservice` 会报告作业服务摘要。

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

`job ls` 会列出正在作业服务上运行或运行过的job。

```console
$ ./bin/alluxio job ls
1613673433925   Persist    COMPLETED
1613673433926   Persist    COMPLETED
1613673433927   Persist    COMPLETED
1613673433928   Persist    COMPLETED
1613673433929   Persist    COMPLETED
```

### job stat -v <job_id> 

`job stat -v <job_id>` 会列出某个Job的详细信息。 （加 -v 表示包含worker 上指定任务的信息）

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