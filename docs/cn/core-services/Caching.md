---
layout: global
title: Alluxio存储管理
nickname: Alluxio存储管理
group: Core Services
priority: 0
---

* 内容列表
{:toc}

Alluxio管理了Alluxio workers节点的包括内存在内的本地存储，来充当分布式缓冲缓存区。这个在用户应用程序和各种底层存储之间的快速数据层可以很大程度上改善I/O性能。

每个Alluxio节点管理的存储数量和类型由用户配置决定。Alluxio还支持层次化存储，这使得系统存储能够感知介质，让数据存储获得类似于L1/L2 cpu缓存的优化。

## 配置Alluxio存储

配置Alluxio存储的最简单方法是使用默认的单层模式。

请注意本文中提到的本地存储以及`mount`这样的术语特指在本地文件系统中挂载，不要与Alluxio底层存储的`mount`概念混为一谈。

默认配置下的Alluxio将为每个worker提供一个ramdisk，并占用一定的百分比的系统总内存。这个ramdisk将被用作分配给每个Alluxio worker的唯一存储介质。

Alluxio存储通过Alluxio的`alluxio-site.properties`配置。详细配置请参考[配置文档]({{ '/cn/operation/Configuration.html' | relativize_url }})。

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

更新存储介质后，我们需要指出为每个存储目录分配多少存储空间。例如，如果我们想在ramdisk上使用16 GB，在每个SSD上使用100 GB，

```
alluxio.worker.tieredstore.level0.dirs.quota=16GB,100GB,100GB
```

请注意配额的序列必须与路径的序列相匹配。

在`alluxio.worker.memory.size`和`alluxio.worker.tieredstore.level0.dirs.quota`（默认值等于前者）之间有一个微妙的区别。Alluxio在使用`Mount`或`SudoMount`选项启动时会提供并安装ramdisk。这个ramdisk无论在`alluxio.worker.tieredstore.level0.dirs.quota`中设置的值如何，其大小都由`alluxio.worker.memory.size`确定。 同样，如果要使用默认的Alluxio提供的ramdisk以外的其他设备，配额应该独立设置于内存大小。

## 回收

因为Alluxio存储被设计成动态变化的，所以必须有一个机制在Alluxio存储已满时为新的数据腾出空间。这被称为回收。

在Alluxio中使用异步回收。它依赖于每个worker的周期性空间预留线程来回收数据。它等待worker存储利用率达到配置的高水位。然后它回收
基于回收策略的数据直到达到配置的低水位。例如，如果我们配置了相同的16 + 100 + 100 = 216GB的存储空间，我们可以将回收设置为200GB左右开始并在160GB左右停止：

```
alluxio.worker.tieredstore.level0.watermark.high.ratio=0.9 # 216GB * 0.9 ~ 200GB
alluxio.worker.tieredstore.level0.watermark.low.ratio=0.75 # 216GB * 0.75 ~ 160GB
```

与Alluxio2.0之前采用的同步回收相比，在写或读缓存高工作负载时异步回收可以提高性能。
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

分层存储的配置与其余Alluxio配置一样，也是在`alluxio-site.properties`中完成。相关内容请参考[配置参数]({{ '/cn/operation/Configuration.html' | relativize_url }})。使用如下配置参数可以指定Alluxio的额外存储层：

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

## 设置生存时间 (TTL)

Alluxio具有与每个文件或目录相关联的TTL属性。这些属性通过日志持久化。保证集群重启后的一致性。
当Alluxio运行时，活跃的master节点负责保存元数据在内存中。在内部，master进程运行一个后台线程定期检查文件是否已经到达它对应的TTL值。

注意，后台线程在一个可配置的时间段内运行，默认为1小时。这意味着TTL在下一次检查间隔前不会强制执行，TTL的强制执行可以达到1
TTL间隔延迟。间隔长度由 `alluxio.master.ttl.checker.interval` 属性设置。

例如，将间隔设置为10分钟，将以下内容添加到 `alluxio-site.properties`:

```
alluxio.master.ttl.checker.interval=10m
```

查看[配置文档]({{ '/cn/operation/Configuration.html' | relativize_url }})获取有关Alluxio设置的更多配置

虽然master节点负责执行TTL，但是TTL值的设置取决于客户端

### 接口

有三种方法可以设置Alluxio的TTL。
1. 通过Alluxio的shell命令行。
1. 通过Alluxio Java文件系统API。
1. 被动加载元数据或创建文件

TTL API如下:

```
SetTTL(path, duration, action)
`path`          Alluxio命名空间中的路径
`duration`      TTL操作生效之前的毫秒数，将覆盖任何之前的值
`action`        生存时间过后要采取的行动。`FREE`将导致文件被逐出Alluxio存储，不管pin状态如何。
                `DELETE`将导致文件从Alluxio命名空间中删除，并在存储中删除。
                注意:`DELETE`是某些命令的默认值，将导致文件被永久删除。
```

### 使用命令行

查看[命令行文档]({{ '/cn/operation/User-CLI.html' | relativize_url }}#setttl).

#### Java 文件系统接口

使用Alluxio文件系统对象来设置具有适当选项的文件属性。

```java
FileSystem alluxioFs = FileSystem.Factory.get();

AlluxioURI path = new AlluxioURI("alluxio://hostname:port/file/path");
long ttlMs = 86400000L; // 1 day
TtlAction ttlAction = TtlAction.FREE; // Free the file when TTL is hit

SetAttributeOptions options = SetAttributeOptions.defaults().setTtl(ttlMs).setTtlAction(ttlAction);
alluxioFs.setAttribute(path);
```

查看[Java 文档](https://docs.alluxio.io/os/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html)获取更多信息

#### 被动加载元数据或创建文件

每当一个新文件被添加到Alluxio命名空间时，用户都可以选择被动添加一个TTL到那个文件。这在用户希望访问的文件被暂时使用时
非常有用。它不是多次调用API，而是自动设置为文件发现。

注意:被动TTL更方便，但也不太灵活。选项是客户端级别的，所以所有选项都是
来自客户端的TTL请求，将具有相同的动作和生存时间。

被动TTL使用以下配置选项:

* `alluxio.user.file.load.ttl` - 针对从底层存储加载到Alluxio中的任何新文件的默认生存时间。默认情况下没有TTL。
* `alluxio.user.file.load.ttl.action` - 任何ttl设置的默认操作，设置在从一个under store加载到Alluxio的文件上。
默认情况下是 `DELETE`。
* `alluxio.user.file.create.ttl` - 新创建的任何文件的默认的生存时间。默认情况下没有TTL。
* `alluxio.user.file.create.ttl.action` - 在一个新创建的文件上的任何ttl设置的默认操作。默认情况下是`DELETE`。

有两对选项，一组用于  `load`，一组用于`create`。`load`指的是Alluxio从底层存储获取的文件。
`Create`指创建的新文件或目录。

这两个选项在默认情况下都是禁用的，应该只由具有严格数据访问模式的客户端启用
例如，要删除`runTests`在1分钟后创建的文件:

```
bin/alluxio runTests -Dalluxio.user.file.create.ttl=1m -Dalluxio.user.file.create.ttl.action=DELETE
```

注意，如果您尝试使用这个示例，请确保将 `alluxio.master.ttl.checker.interval` 设置为较短时间，即1分钟。
