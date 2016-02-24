---
layout: global
title: 度量指标系统
group: Features
priority: 3
---

* Table of Contents
{:toc}

度量指标信息可以让用户深入了解集群上运行的任务。这些信息对于监控和调试是宝贵的资源。Alluxio有一个基于[Coda Hale Metrics库](https://github.com/dropwizard/metrics)的可配置的度量指标系统。度量指标系统中，度量指标源就是该度量指标信息生成的地方，度量指标槽会使用由度量指标源生成的记录。度量指标检测系统会周期性地投票决定度量指标源，并将度量指标记录传递给度量指标槽。

Alluxio的度量指标信息被分配到各种相关Alluxio组件的实例中。每个实例中，用户可以配置一组度量指标槽，来决定报告哪些度量指标信息。现在支持下面的实例：

* Master: Alluxio独立（standalone）Master进程。
* Worker: Alluxio独立（standalone）Worker进程。

每个实例可以报告零个或多个度量指标槽。

* ConsoleSink: 输出控制台的度量值。
* CsvSink: 每隔一段时间将度量指标信息导出到CSV文件中。
* JmxSink: 查看JMX控制台中寄存器的度量信息。
* GraphiteSink: 给Graphite服务器发送度量信息。
* MetricsServlet: 添加Web UI中的servlet，作为JSON数据来为度量指标数据服务。

一些类似于`BytesReadLocal`的度量指标信息依赖于从Client心跳中收集的信息。为了获取精确的度量信息，Client会在使用完这些信息后，适当地关闭该`AlluxioFileSystem`客户端实例。

# 配置
度指标量系统可以通过配置文件进行配置，Alluxio中该文件默认位于`$ALLUXIO_HOME/conf/metrics.properties`。自定义文件位置可以通过`alluxio.metrics.conf.file`配置项来指定。Alluxio在conf目录下提供了一个metrics.properties.template文件，其包括所有可配置属性。默认情况下，MetricsServlet是生效的，你可以发送HTTP请求"/metrics/json"来获取一个以JSON格式表示的所有已注册度量信息的快照。

# 支持的度量指标信息

度量指标信息可以被分为：

* 常规信息: 集群的整体度量信息（如：CapacityTotal）。
* 逻辑操作: 执行的操作数量（如：FilesCreated）。
* RPC调用: 每个操作的RPC调用次数（如：CreateFileOps）。

下面详细展示了可用的度量指标信息。

## Master

### 常规信息

* CapacityTotal: 文件系统总容量（以字节为单位）。
* CapacityUsed: 文件系统中已使用的容量（以字节为单位）。
* CapacityFree: 文件系统中未使用的容量（以字节为单位）。
* PathsTotal: 文件系统中文件和目录的数目。
* UnderFsCapacityTotal: 底层文件系统总容量（以字节为单位）。
* UnderFsCapacityUsed: 底层文件系统中已使用的容量（以字节为单位）。
* UnderFsCapacityFree: 底层文件系统中未使用的容量（以字节为单位）。
* Workers: Worker的数目。

### 逻辑操作

* DirectoriesCreated: 创建的目录数目。
* FileBlockInfosGot: 被检索的文件块数目。
* FileInfosGot: 被检索的文件数目。
* FilesCompleted: 完成的文件数目。
* FilesCreated: 创建的文件数目。
* FilesFreed: 释放掉的文件数目。
* FilesPersisted: 持久化的文件数目。
* FilesPinned: 被固定的文件数目。
* NewBlocksGot: 获得的新数据块数目。
* PathsDeleted: 删除的文件和目录数目。
* PathsMounted: 挂载的路径数目。
* PathsRenamed: 重命名的文件和目录数目。
* PathsUnmounted: 未被挂载的路径数目。

### RPC调用

* CompleteFileOps: CompleteFile操作的数目。
* CreateDirectoryOps: CreateDirectory操作的数目。
* CreateFileOps: CreateFile操作的数目。
* DeletePathOps: DeletePath操作的数目。
* FreeFileOps: FreeFile操作的数目。
* GetFileBlockInfoOps: GetFileBlockInfo操作的数目。
* GetFileInfoOps: GetFileInfo操作的数目。
* GetNewBlockOps: GetNewBlock操作的数目。
* MountOps: Mount操作的数目。
* RenamePathOps: RenamePath操作的数目。
* SetStateOps: SetState操作的数目。
* UnmountOps: Unmount操作的数目。

## Worker

### 常规信息

* CapacityTotal: 该Worker的总容量（以字节为单位）。
* CapacityUsed: 该Worker已使用的容量（以字节为单位）。
* CapacityFree: 该Worker未使用的容量（以字节为单位）。

### 逻辑操作

* BlocksAccessed: 访问的数据块数目。
* BlocksCached: 被缓存的数据块数目。
* BlocksCanceled: 被取消的数据块数目。
* BlocksDeleted: 被删除的数据块数目。
* BlocksEvicted: 被替换的数据块数目。
* BlocksPromoted: 被提升到内存的数据块数目。
* BlocksReadLocal: 从本地Worker读取的数据块数目。
* BlocksReadRemote: 从远程Worker上读取的数据块数目。
* BlocksWrittenLocal: 写到本地Worker上的数据块数目。
* BytesReadLocal: 从本地Worker读取的字节数。
* BytesReadRemote: 从远程Worker上读取的字节数。
* BytesReadUfs: 从本地Worker的底层文件系统上读取的字节数。
* BytesWrittenLocal: 写到本地Worker上的字节数。
* BytesWrittenUfs: 写到本地Worker的底层文件系统上的字节数。
