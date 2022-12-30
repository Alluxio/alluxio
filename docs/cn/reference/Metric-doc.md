---
layout: global
title: List of Metrics
group: Reference
priority: 1
---

* Table of Contents
{:toc}

在 Alluxio 中，有两种类型的指标，集群范围内的合计指标和每个进程的详细指标。


* 合计指标由领头的 master 收集和计算的，并且在 web UI 下的指标标签下展示。
   这些指标旨在提供 Alluxio 服务的集群状态以及数据与元数据总量的快照。

* 进程指标由每个 Alluxio 进程收集，并通过任何配置的接收器以机器可读的格式暴露出来。

  进程指标高度详细，旨在被第三方监测工具使用。
  用户可以通过细粒度的数据面板查看每个指标的时间序列图。
  比如数据传输量或 RPC 调用次数。

Metrics in Alluxio have the following format for master node metrics:
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
    <td>{{ site.data.table.en.process-metrics[item.metricName] }}</td>
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

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Master.CapacityTotalTier{TIER_NAME} | Total capacity in tier {TIER_NAME} of the Alluxio file system in bytes |
| Master.CapacityUsedTier{TIER_NAME}  | Used capacity in tier {TIER_NAME} of the Alluxio file system in bytes |
| Master.CapacityFreeTier{TIER_NAME}  | Free capacity in tier {TIER_NAME} of the Alluxio file system in bytes |
| Master.UfsSessionCount-Ufs:{UFS_ADDRESS} | The total number of currently opened UFS sessions to connect to the given {UFS_ADDRESS} |
| Master.{UFS_RPC_NAME}.UFS:{UFS_ADDRESS}.UFS_TYPE:{UFS_TYPE}.User:{USER} | The details UFS rpc operation done by the current master |
| Master.PerUfsOp{UFS_RPC_NAME}.UFS:{UFS_ADDRESS} | The aggregated number of UFS operation {UFS_RPC_NAME} ran on UFS {UFS_ADDRESS} by leading master |  
| Master.{LEADING_MASTER_RPC_NAME} | The duration statistics of RPC calls exposed on leading master |

## Worker 指标

默认 worker 指标:

<table class="table table-striped">
<tr><th>名称</th><th>类型</th><th>描述</th></tr>
{% for item in site.data.table.worker-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.worker-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

动态的 worker 指标:

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Worker.UfsSessionCount-Ufs:{UFS_ADDRESS} | The total number of currently opened UFS sessions to connect to the given {UFS_ADDRESS} |
| Worker.{RPC_NAME}                        | The duration statistics of RPC calls exposed on workers |

## Client 指标

每个客户端度量将使用其本地主机名或配置的 alluxio.user.app.id 进行记录。
如果配置了 `alluxio.user.app.id`，多个客户端可以组合成一个逻辑应用。

<table class="table table-striped">
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.client-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.client-metrics[item.metricName] }}</td>
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
<tr><th>Name</th><th>Type</th><th>Description</th></tr>
{% for item in site.data.table.fuse-metrics %}
  <tr>
    <td><a class="anchor" name="{{ item.metricName }}"></a> {{ item.metricName }}</td>
    <td>{{ item.metricType }}</td>
    <td>{{ site.data.table.en.fuse-metrics[item.metricName] }}</td>
  </tr>
{% endfor %}
</table>

Fuse 读/写文件数量可用作 Fuse 应用程序压力的指标。
如果在短时间内发生大量并发读/写操作，则每个读/写操作可能需要更长的时间来完成。

当用户或应用程序在 Fuse 挂载点下运行文件系统命令时，该命令将由操作系统处理和转换，并触发在 [AlluxioFuse](https://github.com/Alluxio/alluxio/blob/db01aae966849e88d342a71609ab3d910215afeb/integration/fuse/src/main/java/alluxio/fuse/AlluxioJniFuseFileSystem.java) 中暴露的相关 Fuse 操作。每个操作被调用的次数以及每次调用的持续时间将使用动态指标名称 `Fuse.<FUSE_OPERATION_NAME>` 记录。


重要的 Fuse 指标包括：

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| Fuse.readdir | The duration metrics of listing a directory |
| Fuse.getattr | The duration metrics of getting the metadata of a file |
| Fuse.open | The duration metrics of opening a file for read or overwrite |
| Fuse.read | The duration metrics of reading a part of a file |
| Fuse.create | The duration metrics of creating a file for write |
| Fuse.write | The duration metrics of writing a file |
| Fuse.release | The duration metrics of closing a file after read or write. Note that release is async so fuse threads will not wait for release to finish |
| Fuse.mkdir | The duration metrics of creating a directory |
| Fuse.unlink | The duration metrics of removing a file or a directory |
| Fuse.rename | The duration metrics of renaming a file or a directory |
| Fuse.chmod | The duration metrics of modifying the mode of a file or a directory |
| Fuse.chown | The duration metrics of modifying the user and/or group ownership of a file or a directory |

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

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| name | The name of the JVM |
| uptime | The uptime of the JVM |
| vendor | The current JVM vendor |

### GC 统计

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| PS-MarkSweep.count | Total number of mark and sweep |
| PS-MarkSweep.time | The time used to mark and sweep |
| PS-Scavenge.count | Total number of scavenge |
| PS-Scavenge.time | The time used to scavenge |

### 内存使用情况

Alluxio 提供整体和详细的内存使用信息。
每个进程中代码缓存、压缩类空间、元数据空间、PS Eden 空间、PS old gen 以及 PS survivor 空间的详细内存使用信息都会被收集。

以下是内存使用指标的子集：

| Metric Name | Description |
|------------------------------|-----------------------------------------------------|
| total.committed | The amount of memory in bytes that is guaranteed to be available for use by the JVM |
| total.init | The amount of the memory in bytes that is available for use by the JVM |
| total.max | The maximum amount of memory in bytes that is available for use by the JVM |
| total.used | The amount of memory currently used in bytes |
| heap.committed | The amount of memory from heap area guaranteed to be available |
| heap.init | The amount of memory from heap area available at initialization |
| heap.max | The maximum amount of memory from heap area that is available |
| heap.usage | The amount of memory from heap area currently used in GB|
| heap.used | The amount of memory from heap area that has been used |
| pools.Code-Cache.used | Used memory of collection usage from the pool from which memory is used for compilation and storage of native code |
| pools.Compressed-Class-Space.used | Used memory of collection usage from the pool from which memory is use for class metadata |
| pools.PS-Eden-Space.used | Used memory of collection usage from the pool from which memory is initially allocated for most objects |
| pools.PS-Survivor-Space.used | Used memory of collection usage from the pool containing objects that have survived the garbage collection of the Eden space |

### 类加载统计

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| loaded | The total number of classes loaded |
| unloaded | The total number of unloaded classes |

### 线程统计

| Metric Name | Description |
|-------------------------|-----------------------------------------------------|
| count | The current number of live threads |
| daemon.count | The current number of live daemon threads |
| peak.count | The peak live thread count |
| total_started.count | The total number of threads started |
| deadlock.count | The number of deadlocked threads |
| deadlock | The call stack of each thread related deadlock |
| new.count | The number of threads with new state |
| blocked.count | The number of threads with blocked state |
| runnable.count | The number of threads with runnable state |
| terminated.count | The number of threads with terminated state |
| timed_waiting.count | The number of threads with timed_waiting state |
