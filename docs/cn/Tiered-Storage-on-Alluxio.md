---
layout: global
title: Alluxio层次化存储
nickname: 分层存储
group: Features
priority: 4
---

* 内容列表
{:toc}

Alluxio支持分层存储，以便管理内存之外的其它存储类型。目前Alluxio支持这些存储类型(存储层)：

* MEM (内存)
* SSD (固态硬盘)
* HDD (硬盘驱动器)

使用带分层存储的Alluxio使得Alluxio可以一次在系统中存储更多数据，因为内存容量在一些部署中非常有限。有了分层存储，Alluxio自动管理存放于不同存储层中的数据块，因此用户和管理员无需手动管理数据存放的位置。用户可以通过实现[分配策略](#allocators) and [回收策略](#evictors)指定自己的数据管理策略。另外，对分层存储进行手动控制也是可行的，参见[固定文件](#pinning-files)。

## 使用分层存储

引入分层存储后，Alluxio管理的数据块不只在内存中，可存放于任何可用的存储层。Alluxio使用*分配策略*和*回收策略*管理块的存放和移动。Alluxio根据I/O性能的高低从上到下配置存储层。因此，这种配置策略决定了最顶层存储是MEM，然后是SSD，最后是HDD。

### 存储目录

一个存储层至少有一个存储目录。目录是Alluxio数据块存放的文件路径。Alluxio支持单个存储层包含多个目录的配置，允许一个存储层有多个挂载点或存储设备。举例而言，如果Alluxio worker上有5个SSD设备，可以配置Alluxio在SSD层同时使用这5个SSD设备。详细配置请参考[下面](#enabling-and-configuring-tiered-storage)。[分配策略](#allocators)决定数据块文件存放的目录。

### 写数据

用户写入新数据块时默认写在顶层存储。如果顶层没有足够的空间存放数据块，回收策略会被触发并释放空间给新数据块。如果顶层没有足够的可释放空间，那么写操作会失败。如果文件大小超出了顶层空间，写操作也会失败。

用户还可以通过[配置项设置](#configuration-parameters-for-tiered-storage)指定写数据默认的层级。

从ReadType.CACHE或ReadType.CACHE_PROMOTE读数据会导致数据被写到Alluxio中。这种情况下，数据被默认写到顶层。

最后，通过load命令可将数据写到Alluxio中。这种情况，数据也会被写到顶层。

### 读数据

读取分层存储的数据块和标准Alluxio类似。如果数据已经在Alluxio中，Alluxio从存储位置读取数据块。如果Alluxio配置了多层存储，数据块不一定是从顶层读取，因为可能被透明地移到下层存储中。

读取策略为ReadType.CACHE_PROMOTE时，Alluxio会确保数据在读取前先被移动到顶层存储中。通过显式的将热数据移到最高层,该策略也可以用于数据块的管理。

#### 固定文件

用户可以通过*固定(pin)*和*取消固定(unpin)*来固定和移动该文件。文件被固定时，数据块不会从Alluxio的存储空间中移出。同时用户可以将固定文件的数据块移到顶层存储。

固定文件的例子：

{% include Tiered-Storage-on-Alluxio/pin-file.md %}

类似地，取消固定文件的例子：

{% include Tiered-Storage-on-Alluxio/unpin-file.md %}

由于已固定文件的数据块不会被移出，client要确保在合适的时候取消固定文件。

## 分配策略

Alluxio使用分配策略选择新数据块的写入位置。Alluxio定义了分配策略的框架，也内置了几种分配策略。以下是Alluxio已实现的分配策略：

* **贪心分配策略**

    分配新数据块到首个有足够空间的存储目录。

* **最大剩余空间分配策略**

    分配数据块到有最大剩余空间的存储目录。

* **轮询调度分配策略**

    分配数据块到有空间的最高存储层，存储目录通过轮询调度选出。

将来会有更多的分配策略可供选择。由于Alluxio支持自定义分配策略。你可以为自己的应用开发合适的分配策略。

### 回收策略

Alluxio使用回收策略决定当空间需要释放时，哪些数据块被移到低存储层。Alluxio支持自定义回收策略，已有的实现包括：

* **贪心回收策略**

    移出任意的块直到释放出所需大小的空间。

* **LRU回收策略**

    移出最近最少使用的数据块直到释放出所需大小的空间。

* **LRFU回收策略**

    基于权重分配的最近最少使用和最不经常使用策略移出数据块。如果权重完全偏向最近最少使用,LRFU回收策略退化为LRU回收策略。

* **部分LRU回收策略**

    基于最近最少使用移出，但是选择有最大剩余空间的存储目录(StorageDir)，只从该目录移出数据块。

将来会有更多的回收策略可供选择。由于Alluxio支持自定义回收策略。你也可以为自己的应用开发合适的回收策略。

使用同步移出时，推荐使用较小的块大小配置（64MB左右），以降低块移出的延迟。使用[空间预留器](#space-reserver)时，块大小不会影响移出延迟。

### 空间预留器

空间预留器可以在该存储空间被完全耗尽前试着在每一层预留一定比例的空间。其用于改善突发写入的性能，也可以提高持续写入的边际性能增益，否则，由于回收策略会持续运行，从而为写数据释放空间，所以突发写可能会比较慢。开启和配置空间预留器参见[配置部分](#enabling-and-configuring-tiered-storage)。

空间预留器可以通过为每一层级配置上下水位来执行。一旦达到高水位，后台的回收策略就会启动以释放空间，直到达到低水位。

## 开启和配置空间预留

在Alluxio中，使用[配置参数](Configuration-Settings.html)开启分层存储。默认情况下，Alluxio使用单层内存存储。使用如下配置参数可以指定Alluxio的额外存储层：

{% include Tiered-Storage-on-Alluxio/configuration-parameters.md %}

举例而言，如果想要配置Alluxio有两级存储--内存和硬盘--，可以使用如下配置：

{% include Tiered-Storage-on-Alluxio/two-tiers.md %}

相关配置说明如下：

* `alluxio.worker.tieredstore.levels=2` 在Alluxio中配置了两级存储
* `alluxio.worker.tieredstore.level0.alias=MEM`配置了首层(顶层)是内存存储层
* `alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk` 定义了`/mnt/ramdisk`是首层的文件路径
* `alluxio.worker.tieredstore.level0.dirs.quota=100GB`设置了ramdisk的配额是`100GB`
* `alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9`设置了顶层的高水位比例是0.9
* `alluxio.worker.tieredstore.level0.watermark.low.ratio=0.7`设置了顶层的低水位比例0.7
* `alluxio.worker.tieredstore.level1.alias=HDD`配置了第二层是硬盘层
* `alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3`配置了第二层3个独立的文件路径
* `alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB`定义了第二层3个文件路径各自的配额
* `alluxio.worker.tieredstore.level1.watermark.high.ratio=0.9`设置了第二层的高水位比例是0.9
* `alluxio.worker.tieredstore.level1.watermark.low.ratio=0.7`设置了第二层的低水位比例0.7

定义存储层时有一些限制。Alluxio对于层级数量不做限制，首先一般是三层——内存，HDD和SDD。最多只有一层可以引用指定的别名。例如：最多有一层可以使用别名HDD。如果想在HDD层使用多个硬盘驱动器，可以配置`alluxio.worker.tieredstore.level{x}.dirs.path`为多个存储路径。

另外，可以配置特定的回收策略和分配策略。配置参数是：

{% include Tiered-Storage-on-Alluxio/evictor-allocator.md %}

空间预留可以配置为开启或关闭：

    alluxio.worker.tieredstore.reserver.enabled=false

## 分层存储的参数配置

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
