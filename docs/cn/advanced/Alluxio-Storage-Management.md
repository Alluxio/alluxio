---
layout: global
title: Alluxio存储管理
nickname: Alluxio存储管理
group: Advanced
priority: 0
---

* 内容列表
{:toc}

Alluxio管理Alluxio workers的本地存储，包括内存，来充当分布式缓冲缓存区。这个在用户应用程序和各种底层存储之间的快速数据层可以很大程度上改善I/O性能。

每个Alluxio节点管理的存储数量和类型由用户配置决定。Alluxio还支持层次化存储，这使得系统存储能够感知介质，让数据存储获得类似于L1/L2 cpu缓存的优化。

## 配置Alluxio存储

配置Alluxio存储的最简单方法是使用默认的单层模式。

请注意本文中提到的本地存储以及`mount`这样的术语特指在本地文件系统中挂载，不要与Alluxio底层存储的`mount`概念混为一谈。

默认配置下的Alluxio将为每个worker提供一个ramdisk，并占用一定的百分比的系统总内存。这个ramdisk将被用作分配给每个Alluxio worker的唯一存储介质。

Alluxio存储通过Alluxio的`alluxio-site.properties`配置。详细配置请参考[配置文档](Configuration-Settings.html)。

默认情况的一个常见修改是显式设置ramdisk的大小。例如，设置每个worker的ramdisk大小为16GB：

```
alluxio.worker.memory.size=16GB
```

另一个常见设置是指定多个存储介质，如ramdisk和SSD。 我们需要更新`alluxio.worker.tieredstore.level0.dirs.path`来指定我们想要的每个存储介质作为存储目录。例如，要使用ramdisk（安装在`/mnt/ramdisk`）和两个
SSD（安装在`/mnt/ssd1`和`/mnt/ssd2`）：

```
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk,/mnt/ssd1,/mnt/ssd2
```

提供的路径应该指向本地文件系统中安装的相应存储介质的路径。为了启动短路操作，这些路径的权限应该允许客户端用户在改路径上读写和执行。例如，与启动Alluxio服务的用户在同一用户组的客户端用户需要770的权限。

更新存储介质后，我们需要指出每个存储分配了多少存储空间。例如，如果我们想在ramdisk上使用16 GB，在每个SSD上使用100 GB，

```
alluxio.worker.tieredstore.level0.dirs.quota=16GB,100GB,100GB
```

请注意配额的序列必须与路径的序列相匹配。

在`alluxio.worker.memory.size`和`alluxio.worker.tieredstore.level0.dirs.quota`（默认值等于前者）之间有一个微妙的区别。Alluxio在使用`Mount`或`SudoMount`选项启动时会提供并安装ramdisk。这个ramdisk无论在`alluxio.worker.tieredstore.level0.dirs.quota`中设置的值如何，其大小都由`alluxio.worker.memory.size`确定。 同样，如果要使用默认的Alluxio提供的ramdisk以外的其他设备，配额应该独立设置于内存大小。

## 回收

因为Alluxio存储被设计成动态变化的，所以必须有一个机制在Alluxio存储已满时为新的数据腾出空间。这被称为回收。

在Alluxio中有两种回收模式，异步（默认）和同步。你可以通过启用和禁用处理异步驱逐的空间预留器在这两者之间切换。
例如，要关闭异步回收：

```
alluxio.worker.tieredstore.reserver.enabled=false
```

异步回收是默认的回收实现。它依赖于每个worker的周期性空间预留线程来回收数据。它等待worker存储利用率达到配置的高水位。然后它回收
基于回收策略的数据直到达到配置的低水位。例如，如果我们配置了相同的16 + 100 + 100 = 216GB的存储空间，我们可以将回收设置为200GB左右开始并在160GB左右停止：

```
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9 # 216GB * 0.9 ~ 200GB
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.75 # 216GB * 0.75 ~ 160GB
```

在写或读缓存高工作负载时，异步回收可以提高性能。

同步回收等待一个客户端请求比当前在worker上可用空间更多的空间，然后启动回收进程来释放足够的空间来满足这一要求。这导致了许多很小的回收尝试，使得效率较低但是使可用的Alluxio空间的利用最大化。

### 回收策略

Alluxio使用回收策略决定当空间需要释放时，哪些数据块被移到低存储层。用户可以指定Alluixo回收策略来获得细粒度地控制回收进程。

Alluxio支持自定义回收策略，已有的实现包括：

* **贪心回收策略(GreedyEvictor)**

    回收任意的块直到释放出所需大小的空间。

* **LRU回收策略(LRUEvictor)**

    回收最近最少使用的数据块直到释放出所需大小的空间。

* **LRFU回收策略(LRFUEvictor)**

    基于权重分配的最近最少使用和最不经常使用策略回收数据块。如果权重完全偏向最近最少使用,LRFU回收策略退化为LRU回收策略。

* **部分LRU回收策略(PartialLRUEvictor)**

    基于最近最少使用回收，但是选择有最大剩余空间的存储目录(StorageDir)，只从该目录回收数据块。

将来会有更多的回收策略可供选择。由于Alluxio支持自定义回收策略。你也可以为自己的应用开发合适的回收策略。

使用同步回收时，推荐使用较小的块大小配置（64-128MB左右），以降低块回收的延迟。使用空间预留器时，块大小不会影响回收延迟。

## 使用分层存储

对于典型的部署，建议使用异构存储介质的单一存储层。但是在某些环境中，基于I/O速度，工作负载将受益于明确的存储介质序列。在这种情况下，应该使用分层存储。当分层存储可用时，回收过程智能地考虑了层概念。Alluxio根据I/O性能的高低从上到下配置存储层。例如，用户经常指定以下几层：

 * MEM (内存)
 * SSD (固态硬盘)
 * HDD (硬盘驱动器)

### 写数据

用户写入新数据块时默认写在顶层存储。如果顶层没有足够的空间存放数据块，回收策略会被触发并释放空间给新数据块。如果顶层没有足够的可释放空间，那么写操作会失败。如果文件大小超出了顶层空间，写操作也会失败。

用户还可以通过[配置项设置](#configuration-parameters-for-tiered-storage)指定写数据默认的层级。

从ReadType.CACHE或ReadType.CACHE_PROMOTE读数据会导致数据被写到Alluxio中。这种情况下，数据被默认写到顶层。

最后，通过load命令可将数据写到Alluxio中。这种情况，数据也会被写到顶层。

### 读数据

读取分层存储的数据块和标准Alluxio类似。如果数据已经在Alluxio中，Alluxio从存储位置读取数据块。如果Alluxio配置了多层存储，数据块不一定是从顶层读取，因为可能被透明地移到下层存储中。

读取策略为ReadType.CACHE_PROMOTE时，Alluxio会确保数据在读取前先被移动到顶层存储中。通过显式的将热数据移到最高层,该策略也可以用于数据块的管理。

### 开启和配置分层存储

在Alluxio中，使用[配置参数](Configuration-Settings.html)开启分层存储。使用如下配置参数可以指定Alluxio的额外存储层：

```
alluxio.worker.tieredstore.levels
alluxio.worker.tieredstore.level{x}.alias
alluxio.worker.tieredstore.level{x}.dirs.quota
alluxio.worker.tieredstore.level{x}.dirs.path
alluxio.worker.tieredstore.level{x}.watermark.high.ratio
alluxio.worker.tieredstore.level{x}.watermark.low.ratio
```

举例而言，如果想要配置Alluxio有两级存储--内存和硬盘--，可以使用如下配置：

```
alluxio.worker.tieredstore.levels=2
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
alluxio.worker.tieredstore.level0.dirs.quota=100GB
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7
alluxio.worker.tieredstore.level1.alias=HDD
alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9
alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7
```

相关配置说明如下：

* `alluxio.worker.tieredstore.levels=2` 在Alluxio中配置了两级存储
* `alluxio.worker.tieredstore.level0.alias=MEM` 配置了首层(顶层)是内存存储层
* `alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` 定义了`/mnt/ramdisk`是首层的文件路径
* `alluxio.worker.tieredstore.level0.dirs.quota=100GB` 设置了ramdisk的配额是`100GB`
* `alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9` 设置了顶层的高水位比例是0.9
* `alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7` 设置了顶层的低水位比例0.7
* `alluxio.worker.tieredstore.level1.alias=HDD` 配置了第二层是硬盘层
* `alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3` 配置了第二层3个独立的文件路径
* `alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB` 定义了第二层3个文件路径各自的配额
* `alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9` 设置了第二层的高水位比例是0.9
* `alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7` 设置了第二层的低水位比例0.7

定义存储层时有一些限制。Alluxio对于层级数量不做限制，首先一般是三层——内存，HDD和SDD。最多只有一层可以引用指定的别名。例如：最多有一层可以使用别名HDD。如果想在HDD层使用多个硬盘驱动器，可以配置`alluxio.worker.tieredstore.level{x}.dirs.path`为多个存储路径。

### 分层存储的参数配置

分层存储配置参数如下：

<table class="table table-striped">
<tr><th>参数</th><th>缺省值</th><th>描述</th></tr>
{% for item in site.data.table.tiered-storage-configuration-parameters %}
<tr>
<td>{{ item.parameter }}</td>
<td>{{ item.defaultValue }}</td>
<td>{{ site.data.table.cn.tiered-storage-configuration-parameters[item.parameter] }}</td>
</tr>
{% endfor %}
</table>
