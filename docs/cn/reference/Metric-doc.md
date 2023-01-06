---
layout: global
title: List of Metrics
group: Reference
priority: 1
---

* Table of Contents
{:toc}

在 Alluxio 中，有两种类型的指标，集群范围内的合计指标和每个进程的详细指标。

* 合计指标由 leading master 收集和计算的，并且在 web UI 下的指标标签下展示。
   这些指标旨在提供 Alluxio 服务的集群状态以及数据与元数据总量的快照。

* 进程指标由每个 Alluxio 进程收集，并通过任何配置的接收器以机器可读的格式暴露出来。

  进程指标高度详细，旨在被第三方监测工具使用。
  用户可以通过细粒度的数据面板查看每个指标的时间序列图。
  比如数据传输量或 RPC 调用次数。

Alluxio 的主节点指标具有以下格式：

```
Master.[metricName].[tag1].[tag2]...
```

Alluxio 的非主节点指标具有以下格式

```
[processType].[metricName].[tag1].[tag2]...[hostName] 
```

通常情况下，Alluxio 会为每一种 RPC 调用生成一个指标，无论是调用 Alluxio 还是调用下层存储。

标签是指标的附加元数据，如用户名或存储位置。
标签可用于进一步筛选或聚合各种特征。

## 集群指标

Worker 和 client 通过心跳包将指标数据发送到 Alluxio master。心跳间隔分别由 `alluxio.master.worker.heartbeat.interval` 和 `alluxio.user.metrics.heartbeat.interval` 属性定义。

字节指标是来自 worker 或 client 的聚合值。字节吞吐量指标是在 master 上计算的。
字节吞吐量的值等于字节指标计数器值除以指标记录时间，并以字节/分钟的形式呈现。

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.cluster-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.cluster-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## 进程指标

所有 Alluxio 服务器和客户端进程共享的指标。

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.process-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.process-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## 服务器指标

Alluxio 服务器共享的指标。

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.server-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.server-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Master 指标

默认 Master 指标:

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.master-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.master-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

动态生成的 Master 指标:

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| Master.CapacityTotalTier{TIER_NAME} | Alluxio 文件系统中层 {TIER_NAME} 以字节为单位的总容量 |
| Master.CapacityUsedTier{TIER_NAME}  | Alluxio 文件系统中层 {TIER_NAME} 以字节为单位已使用的容量 |
| Master.CapacityFreeTier{TIER_NAME}  | Alluxio 文件系统中层 {TIER_NAME} 以字节为单位未使用的容量 |
| Master.UfsSessionCount-Ufs:{UFS_ADDRESS} | 当前打开并连接到给定 {UFS_ADDRESS} 的 UFS 会话数 |
| Master.{UFS_RPC_NAME}.UFS:{UFS_ADDRESS}.UFS_TYPE:{UFS_TYPE}.User:{USER} | 当前 master 完成的 UFS RPC 操作细节 |
| Master.PerUfsOp{UFS_RPC_NAME}.UFS:{UFS_ADDRESS} | 当前主 master 在 UFS {UFS_ADDRESS} 上运行的 UFS 操作 {UFS_RPC_NAME} 的总数 | 
| Master.{LEADING_MASTER_RPC_NAME} | 主 master 上暴露的 RPC 调用的持续时间统计信息 |

## Worker 指标

默认 worker 指标:

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.worker-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.worker-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

动态的 worker 指标:

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| Worker.UfsSessionCount-Ufs:{UFS_ADDRESS} | 当前打开并连接到给定 {UFS_ADDRESS} 的 UFS 会话数 |
| Worker.{RPC_NAME}                        | worker 上暴露的 RPC 调用的持续时间统计信息 |

## Client 指标

每个客户端度量将使用其本地主机名或配置的 `alluxio.user.app.id` 进行记录。
如果配置了 `alluxio.user.app.id`，多个客户端可以组合成一个逻辑应用。

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.client-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.client-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

## Fuse 指标

Fuse 是长期运行的 Alluxio 客户端。 
根据启动方式，Fuse 指标将显示为：
* 当文件系统客户端在独立的 AlluxioFuse 进程中启动时，显示为客户端指标。
* 当 Fuse 客户端嵌入在 AlluxioWorker 进程中时，显示为 worker 指标。

Fuse metrics includes:

<table class="table table-striped">
<tr><th>描述</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.fuse-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.cn.fuse-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

Fuse 读/写文件数量可用作 Fuse 应用程序压力的指标。
如果在短时间内发生大量并发读/写操作，则每个读/写操作可能需要更长的时间来完成。

当用户或应用程序在 Fuse 挂载点下运行文件系统命令时，该命令将由操作系统处理和转换，并触发在 [AlluxioFuse](https://github.com/Alluxio/alluxio/blob/db01aae966849e88d342a71609ab3d910215afeb/integration/fuse/src/main/java/alluxio/fuse/AlluxioJniFuseFileSystem.java) 中暴露的相关 Fuse 操作。每个操作被调用的次数以及每次调用的持续时间将使用动态指标名称 `Fuse.<FUSE_OPERATION_NAME>` 记录。


重要的 Fuse 指标包括：

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| Fuse.readdir | 列出目录的持续时间指标 |
| Fuse.getattr | 获取文件元数据的持续时间指标 |
| Fuse.open | 打开文件进行读或覆写的持续时间指标 |
| Fuse.read | 读取文件的一部分的持续时间指标 |
| Fuse.create | 为了写入创建文件的持续时间指标 |
| Fuse.write | 写入文件的持续时间指标 |
| Fuse.release | 在读取或写入后关闭文件的持续时间指标。请注意，释放是异步的，因此 FUSE 线程不会等待释放完成 |
| Fuse.mkdir | 创建目录的持续时间指标 |
| Fuse.unlink | 删除文件或目录的持续时间指标 |
| Fuse.rename | 重命名文件或目录的持续时间指标 |
| Fuse.chmod | 更改文件或目录模式的持续时间指标 |
| Fuse.chown | 修改文件或目录的用户和/或组所有权的持续时间指标 |

Fuse相关的指标包括:
* `Client.TotalRPCClients` 显示用于连接到或可连接到 master 或 worker 进行操作的 RPC 客户端的总数。
* 带有 `Direct` 关键字的 worker 指标。当 Fuse 嵌入到 worker 进程中时，它可以通过 worker 内部 API 从该 worker 读取/写入。

相关指标以 `Direct` 结尾。例如，`Worker.BytesReadDirect` 显示该 worker 为其嵌入的 Fuse 客户端提供读取的字节数。
* 如果配置了 `alluxio.user.block.read.metrics.enabled=true`，则会记录 `Client.BlockReadChunkRemote`。 该指标显示通过 gRPC 从远程 worker 读取数据的持续时间统计。

`Client.TotalRPCClients` 和 `Fuse.TotalCalls` 指标是 Fuse 应用程序当前负载的优秀指标。
如果在 Alluxio Fuse 上运行应用程序（e.g. Tensorflow），但这两个指标值比之前低得多，则训练作业可能会卡在 Alluxio 上。

## 普通进程指标

在每个实例（Master、Worker 或 Client）上收集的指标。

### JVM Attributes

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| name | JVM 名称 |
| uptime | JVM 的运行时间 |
| vendor | 当前的 JVM 供应商 |

### GC 统计

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| PS-MarkSweep.count | 标记和清除 old gen 的总数 |
| PS-MarkSweep.time | 标记和清除 old gen 的总时间 |
| PS-Scavenge.count | 清除 young gen 总数 |
| PS-Scavenge.time | 清除 young gen 总时间 |

### 内存使用情况

Alluxio 提供整体和详细的内存使用信息。
每个进程中代码缓存、压缩类空间、元数据空间、PS Eden 空间、PS old gen 以及 PS survivor 空间的详细内存使用信息都会被收集。

以下是内存使用指标的子集：

| 名称 | 描述 |
|------------------------------|-----------------------------------------------------|
| total.committed | 保证可供 JVM 使用的以字节为单位的内存数量 |
| total.init | 可供 JVM 使用的以字节为单位的内存数量 |
| total.max | 以字节为单位的 JVM 可用的最大内存量 |
| total.used | 以字节为单位当前使用的内存大小 |
| heap.committed | 在堆上保证可用的内存大小 |
| heap.init | 初始化时堆上可用的内存量 |
| heap.max | 在堆上可用的最大内存量 |
| heap.usage | 堆上当前正在使用的以 GB 为单位的内存量 |
| heap.used | 堆上当前已经使用过的以 GB 为单位的内存量 |
| pools.Code-Cache.used | 内存池中用于编译和存储本地代码的内存总量 |
| pools.Compressed-Class-Space.used | 内存池中用于类元数据的内存总量 |
| pools.PS-Eden-Space.used | 内存池中用于大多数对象初始分配的内存总量 |
| pools.PS-Survivor-Space.used | 从包含在 Eden space 的垃圾回收中幸存下来的对象的池中使用的内存总量 |

### 类加载统计

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| loaded | 加载的类总数 |
| unloaded | 未加载的类总量 |

### 线程统计

| 名称 | 描述 |
|-------------------------|-----------------------------------------------------|
| count | 当前存活线程数 |
| daemon.count | 当前守护线程的数量 |
| peak.count | 存活线程数峰值 |
| total_started.count | 启动线程总数 |
| deadlock.count | 死锁线程总数 |
| deadlock | 与每个线程有关的死锁的调用栈 |
| new.count | 有新状态的线程数 |
| blocked.count | 阻塞态线程数 |
| runnable.count | 可运行状态线程数 |
| terminated.count | 终结态线程数 |
| timed_waiting.count | 定时等待状态的线程数量 |
