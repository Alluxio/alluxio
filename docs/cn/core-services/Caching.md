---
layout: global
title: 缓存
nickname: 缓存
group: Core Services
priority: 1
---

* Table of Contents
{:toc}

## Alluxio存储概述

本文档的目的是向用户介绍Alluxio存储和
在Alluxio存储空间中可以执行的操作背后的概念。 与元数据相关的操作
例如同步和名称空间，请参阅
[有关命名空间管理的页面]
({{ '/en/core-services/Unified-Namespace.html' | relativize_url }})

Alluxio在帮助统一跨各种平台用户数据的同时还有助于为用户提升总体I / O吞吐量。 
Alluxio是通过把存储分为两个不同的类别来实现这一目标的。

- **UFS(底层文件存储，也称为底层存储)**
    -该存储空间代表不受Alluxio管理的空间。
    UFS存储可能来自外部文件系统，包括如HDFS或S3。
    Alluxio可能连接到一个或多个UFS并在一个命名空间中统一呈现这类底层存储。
    -通常，UFS存储旨在相当长一段时间持久存储大量数据。
- **Alluxio存储**
    - Alluxio做为一个分布式缓存来管理Alluxio workers本地存储，包括内存。这个在用户应用程序与各种底层存储之间的快速数据层带来的是显著提高的I / O性能。
    - Alluxio存储主要用于存储热数据，暂态数据，而不是长期持久数据存储。
    - 每个Alluxio节点要管理的存储量和类型由用户配置决定。
    - 即使数据当前不在Alluxio存储中，通过Alluxio连接的UFS​​中的文件仍然
    对Alluxio客户可见。当客户端尝试读取仅可从UFS获得的文件时数据将被复制到Alluxio存储中。

![Alluxio存储图]({{ '/img/stack.png' | relativize_url }})

Alluxio存储通过将数据存储在计算节点内存中来提高性能。
Alluxio存储中的数据可以被复制来形成“热”数据，更易于I/O并行操作和使用。

Alluxio中的数据副本独立于UFS中可能已存在的副本。
Alluxio存储中的数据副本数是由集群活动动态决定的。
由于Alluxio依赖底层文件存储来存储大部分数据，
Alluxio不需要保存未使用的数据副本。

Alluxio还支持让系统存储软件可感知的分层存储，使类似L1/L2 CPU缓存一样的数据存储优化成为可能。

## 配置Alluxio存储

### 单层存储

配置Alluxio存储的最简单方法是使用默认的单层模式。

请注意，此部分是讲本地存储，诸如`mount`之类的术语指在本地存储文件系统上挂载，不要与Alluxio的外部底层存储的`mount`概念混淆。

在启动时，Alluxio将在每个worker节点上发放一个ramdisk并占用一定比例的系统的总内存。 
此ramdisk将用作分配给每个Alluxio worker的唯一存储介质。

通过Alluxio配置中的`alluxio-site.properties`来配置Alluxio存储。
详细信息见[configuration settings]({{ '/en/operation/Configuration.html' | relativize_url}})。

对默认值的常见修改是明确设置ramdisk的大小。 例如，设置每个worker的ramdisk大小为16GB：

```properties
alluxio.worker.ramdisk.size=16GB
```

另一个常见更改是指定多个存储介质，例如ramdisk和SSD。 需要
更新`alluxio.worker.tieredstore.level0.dirs.path`以指定想用的每个存储介质
为一个相应的存储目录。 例如，要使用ramdisk(挂载在`/mnt/ramdisk`上)和两个
SSD(挂载在`/mnt/ssd1`和`/mnt/ssd2`):

```properties
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk,/mnt/ssd1,/mnt/ssd2
alluxio.worker.tieredstore.level0.dirs.mediumtype=MEM,SSD,SSD
```

请注意，介质类型的顺序必须与路径的顺序相符。
MEM和SSD是Alluxio中的两种预配置存储类型。
`alluxio.master.tieredstore.global.mediumtype`是包含所有可用的介质类型的配置参数，默认情况下设置为`MEM，SSD，HDD`。 
如果用户有额外存储介质类型可以通过修改这个配置来增加。

提供的路径应指向挂载适当存储介质的本地文件系统中的路径。 
为了实现短路操作，对于这些路径，应允许客户端用户在这些路径上进行读取，写入和执行。 
例如，对于与启动Alluxio服务的用户组同组用户应给予`770`权限。

更新存储介质后，需要指出每个存储目录分配了多少存储空间。 
例如，如果要在ramdisk上使用16 GB，在每个SSD上使用100 GB：

```properties
alluxio.worker.tieredstore.level0.dirs.quota=16GB,100GB,100GB
```

注意存储空间配置的顺序一定与存储目录的配置相符。

Alluxio在通过`Mount`或`SudoMount`选项启动时，配置并挂载ramdisk。
这个ramdisk大小是由`alluxio.worker.ramdisk.size`确定的。
默认情况下，tier 0设置为MEM并且管理整个ramdisk。
此时`alluxio.worker.tieredstore.level0.dirs.quota`的值同`alluxio.worker.ramdisk.size`一样。
如果tier0要使用除默认的ramdisk以外的设备，应该显式地设置`alluxio.worker.tieredstore.level0.dirs.quota`选项。

### 多层存储

通常建议异构存储介质也使用单个存储层。
在特定环境中，工作负载将受益于基于I/O速度存储介质明确排序。
Alluxio假定根据按I/O性能从高到低来对多层存储进行排序。
例如，用户经常指定以下层：

  * MEM(内存)
  * SSD(固态存储)
  * HDD(硬盘存储)

#### 写数据

用户写新的数据块时，默认情况下会将其写入顶层存储。如果顶层没有足够的可用空间，
则会尝试下一层存储。如果在所有层上均未找到存储空间，因Alluxio的设计是易失性存储，Alluxio会释放空间来存储新写入的数据块。
根据块注释策略，空间释放操作会从work中释放数据块。 [块注释政策](#block-annotation-policies)。
如果空间释放操作无法释放新空间，则写数据将失败。

**注意:**新的释放空间模型是同步模式并会代表要求为其要写入的数据块释放新空白存储空间的客户端来执行释放空间操作。
在块注释策略的帮助下，同步模式释放空间不会引起性能下降，因为总有已排序的数据块列表可用。
然而，可以将`alluxio.worker.tieredstore.free.ahead.bytes`(默认值：0)配置为每次释放超过释放空间请求所需字节数来保证有多余的已释放空间满足写数据需求。

用户还可以通过[configuration settings](#configuring-tiered-storage)来指定写入数据层。

#### 读取数据

如果数据已经存在于Alluxio中，则客户端将简单地从已存储的数据块读取数据。
如果将Alluxio配置为多层，则不一定是从顶层读取数据块，
因为数据可能已经透明地挪到更低的存储层。

用`ReadType.CACHE_PROMOTE`读取数据将在从worker读取数据前尝试首先将数据块挪到
顶层存储。也可以将其用作为一种数据管理策略
明确地将热数据移动到更高层存储读取。

#### 配置分层存储

可以使用以下方式在Alluxio中启用分层存储
[配置参数]({{ '/en/operation/Configuration.html' | relativize_url}})。
为Alluxio指定额外存储层，使用以下配置参数：

```properties
alluxio.worker.tieredstore.levels
alluxio.worker.tieredstore.level{x}.alias
alluxio.worker.tieredstore.level{x}.dirs.quota
alluxio.worker.tieredstore.level{x}.dirs.path
alluxio.worker.tieredstore.level{x}.dirs.mediumtype
```

例如，如果计划将Alluxio配置为具有两层存储，内存和硬盘存储，
可以使用类似于以下的配置：

```properties
# configure 2 tiers in Alluxio
alluxio.worker.tieredstore.levels=2
# the first (top) tier to be a memory tier
alluxio.worker.tieredstore.level0.alias=MEM
# defined `/mnt/ramdisk` to be the file path to the first tier
alluxio.worker.tieredstore.level0.dirs.path=/mnt/ramdisk
# defined MEM to be the medium type of the ramdisk directory
alluxio.worker.tieredstore.level0.dirs.mediumtype=MEM
# set the quota for the ramdisk to be `100GB`
alluxio.worker.tieredstore.level0.dirs.quota=100GB
# configure the second tier to be a hard disk tier
alluxio.worker.tieredstore.level1.alias=HDD
# configured 3 separate file paths for the second tier
alluxio.worker.tieredstore.level1.dirs.path=/mnt/hdd1,/mnt/hdd2,/mnt/hdd3
# defined HDD to be the medium type of the second tier
alluxio.worker.tieredstore.level1.dirs.mediumtype=HDD,HDD,HDD
# define the quota for each of the 3 file paths of the second tier
alluxio.worker.tieredstore.level1.dirs.quota=2TB,5TB,500GB
```

可配置层数没有限制
但是每个层都必须使用唯一的别名进行标识。
典型的配置将具有三层，分别是内存，SSD和HDD。
要在HDD层中使用多个硬盘存储，需要在配置`alluxio.worker.tieredstore.level{x}.dirs.path`时指定多个路径。

### 块注释策略

Alluxio从v2.3开始使用块注释策略来维护存储中数据块的严格顺序。 
注释策略定义了跨层块的顺序，并在以下操作过程中进行用来参考:
-释放空间
-[动态块放置](#block-aligning-dynamic-block-placement)。

与写操作同步发生的释放空间操作将尝试根据块注释策略强制顺序删除块并释放其空间给写操作。注释顺序的最后一个块是第一个释放空间候选对象，无论它位于哪个层上。

开箱即用的注释策略实施包括:

- **LRUAnnotator**:根据最近最少使用的顺序对块进行注释和释放。
**这是Alluxio的默认注释策略**。
- **LRFUAnnotator**:根据配置权重的最近最少使用和最不频繁使用的顺序对块进行注释。
    - 如果权重完全偏设为最近最少使用，则行为将
    与LRUAnnotator相同。
    - 适用的配置属性包括`alluxio.worker.block.annotator.lrfu.step.factor`
    `alluxio.worker.block.annotator.lrfu.attenuation.factor`。

workers选择使用的注释策略由Alluxio属性
[alluxio.worker.block.annotator.class]({{ '/en/reference/Properties-List.html' | relativize_url}}#alluxio.worker.block.annotator.class)决定。
该属性应在配置中指定完全验证的策略名称。当前可用的选项有:

- `alluxio.worker.block.annotator.LRUAnnotator`
- `alluxio.worker.block.annotator.LRFUAnnotator`

#### 释放空间模拟
旧的释放空间策略和Alluxio提供的实施现在已去掉了，并用适当的注释策略替换。
配置旧的Alluxio释放空间策略将导致worker启动失败，并报错`java.lang.ClassNotFoundException`。
同样，旧的基于水位标记配置已失效。因此，以下配置选项是无效的:

-`alluxio.worker.tieredstore.levelX.watermark.low.ratio`
-`alluxio.worker.tieredstore.levelX.watermark.high.ratio`

然而，Alluxio支持基于自定义释放空间实施算法数据块注释的仿真模式。该仿真模式假定已配置的释放空间策略创建一个基于某种顺序释放空间的计划，并通过定期提取这种自定义顺序来支持块注释活动。

旧的释放空间配置应进行如下更改。 (由于旧的释放空间实施已删除，如未能更改基于旧实施的以下配置就会导致class加载错误。)

-LRUEvictor-> LRUAnnotator
-GreedyEvictor-> LRUAnnotator
-PartialLRUEvictor-> LRUAnnotator
-LRFUEvictor-> LRFUAnnotator

### 分层存储管理
因为块分配/释放不再强制新的写入必须写到特定存储层，新数据块可能最终被写到任何已配置的存储层中。这样允许写入超过Alluxio存储容量的数据。但是，这就需要Alluxio动态管理块放置。
为了确保层配置为从最快到最慢的假设，Alluxio会基于块注释策略在各层存储之间移动数据块。

每个单独层管理任务都遵循以下配置:
- `alluxio.worker.management.task.thread.count`:管理任务所用线程数。 (默认值:CPU核数)
- `alluxio.worker.management.block.transfer.concurrency.limit`:可以同时执行多少个块传输。 (默认:`CPU核数`/2)

#### 块对齐(动态块放置)
Alluxio将动态地跨层移动数据块，以使块组成与配置的块注释策略一致。

为辅助块对齐，Alluxio会监视I/O模式并会跨层重组数据块，以确保
**较高层的最低块比下面层的最高块具有更高的次序**。

这是通过“对齐”这个管理任务来实现的。此管理任务在检测到层之间
顺序已乱时，会通过在层之间交换块位置来有效地将各层与已配置的注释策略对齐以消除乱序。
有关如何控制这些新的后台任务对用户I/O的影响，参见[管理任务推后](#management-task-back-off)部分。

用于控制层对齐:
- `alluxio.worker.management.tier.align.enabled`:是否启用层对齐任务。 (默认: `true`)
- `alluxio.worker.management.tier.align.range`:单个任务运行中对齐多少个块。 (默认值:`100`)
- `alluxio.worker.management.tier.align.reserved.bytes`:配置多层时，默认情况下在所有目录上保留的空间大小。 (默认:1GB)
用于内部块移动。
- `alluxio.worker.management.tier.swap.restore.enabled`:控制一个特殊任务，该任务用于在内部保留空间用尽时unblock层对齐。 (默认:true)
由于Alluxio支持可变的块大小，因此保留空间可能会用尽，因此，当块大小不匹配时在块对齐期间在层之间块交换会导致一个目录保留空间的减少。

#### 块升级
当较高层具有可用空间时，低层的块将向上层移动，以便更好地利用较快的磁盘介质，因为假定较高的层配置了较快的磁盘介质。

用于控制动态层升级:
- `alluxio.worker.management.tier.promote.enabled`:是否启用层升级任务。 (默认: `true`)
- `alluxio.worker.management.tier.promote.range`:单个任务运行中升级块数。 (默认值:`100`)
- `alluxio.worker.management.tier.promote.quota.percent`:每一层可以用于升级最大百分比。
一旦其已用空间超过此值，向此层升级将停止。 (0表示永不升级，100表示总是升级。)

#### 管理任务推后
层管理任务(对齐/升级)会考虑用户I/O并在worker/disk重载情况下推后运行。
这是为了确保内部管理任务不会对用户I/O性能产生负面影响。

可以在`alluxio.worker.management.backoff.strategy`属性中设置两种可用的推后类型，分别是Any和DIRECTORY。

-`ANY`; 当有任何用户I/O时，worker管理任务将推后。 
此模式将确保较低管理任务开销，以便提高即时用户I/O性能。
但是，管理任务要取得进展就需要在worker上花更长的时间。

-`DIRECTORY`; 管理任务将从有持续用户I/O的目录中推后。
此模式下管理任务更易取得进展。
但是，由于管理任务活动的增加，可能会降低即时用户I/O吞吐量。

影响这两种推后策略的另一个属性是`alluxio.worker.management.load.detection.cool.down.time`，控制多长时间的用户I/O计为在目标directory/worker上的一个负载。

## Alluxio中数据生命周期管理

用户需要理解以下概念，以正确利用可用资源:

- **free**:释放数据是指从Alluxio缓存中删除数据，而不是从底层UFS中删除数据。
释放操作后，数据仍然可供用户使用，但对Alluxio释放文件后尝试访问该文件
的客户端来讲性能可能会降低。
- **load**:加载数据意味着将其从UFS复制到Alluxio缓存中。如果Alluxio使用
基于内存的存储，加载后用户可能会看到I/O性能的提高。
- **persist**:持久数据是指将Alluxio存储中可能被修改过或未被修改过的数据写回UFS。
通过将数据写回到UFS，可以保证如果Alluxio节点发生故障数据还是可恢复的。
- **TTL(Time to Live)**:TTL属性设置文件和目录的生存时间，以
在数据超过其生存时间时将它们从Alluxio空间中删除。还可以配置
TTL来删除存储在UFS中的相应数据。

### 从Alluxio存储中释放数据

为了在Alluxio中手动释放数据，可以使用`./bin/alluxio`文件系统命令
行界面。

```console
$ ./bin/alluxio fs free ${PATH_TO_UNUSED_DATA}
```

这将从Alluxio存储中删除位于给定路径的数据。如果数据是持久存储到UFS的则仍然可以访问该数据。有关更多信息，参考
[命令行界面文档]({{ '/en/operation/User-CLI.html' | relativize_url}}#free)

注意，用户通常不需要手动从Alluxio释放数据，因为
配置的[注释策略](#block-annotation-policies)将负责删除未使用或旧数据。

### 将数据加载到Alluxio存储中

如果数据已经在UFS中，使用
[`alluxio fs load`]({{ '/en/operation/User-CLI.html' | relativize_url}}#load)

```console
$ ./bin/alluxio fs load ${PATH_TO_FILE}
```

要从本地文件系统加载数据，使用命令
[`alluxio fs copyFromLocal`]({{ '/en/operation/User-CLI.html' | relativize_url}}#copyfromlocal)。
这只会将文件加载到Alluxio存储中，而不会将数据持久保存到UFS中。
将写入类型设置为`MUST_CACHE`写入类型将不会将数据持久保存到UFS，
而设置为`CACHE`和`CACHE_THROUGH`将会持久化保存。不建议手动加载数据，因为，当首次使用文件时Alluxio会自动将数据加载到Alluxio缓存中。

### 在Alluxio中持久化保留数据

命令[`alluxio fs persist`]({{ '/en/operation/User-CLI.html' | relativize_url}}#persist)
允许用户将数据从Alluxio缓存推送到UFS。

```console
$ ./bin/alluxio fs persist ${PATH_TO_FILE}
```

如果您加载到Alluxio的数据不是来自已配置的UFS，则上述命令很有用。
在大多数情况下，用户不必担心手动来持久化保留数据。

### 设置生存时间(TTL)

Alluxio支持命名空间中每个文件和目录的”生存时间(TTL)”设置。此
功能可用于有效地管理Alluxio缓存，尤其是在严格
保证数据访问模式的环境中。例如，如果对上一周提取数据进行分析，
则TTL功能可用于明确刷新旧数据，从而为新文件释放缓存空间。

Alluxio具有与每个文件或目录关联的TTL属性。这些属性将保存为
日志的一部分，所以集群重新后也能持久保持。活跃master节点负责
当Alluxio提供服务时将元数据保存在内存中。在内部，master运行一个后台
线程，该线程定期检查文件是否已达到其TTL到期时间。

注意，后台线程按配置的间隔运行，默认设置为一个小时。
在检查后立即达到其TTL期限的数据不会马上删除，
而是等到一个小时后下一个检查间隔才会被删除。

如将间隔设置为10分钟，在`alluxio-site.properties`添加以下配置：

```properties
alluxio.master.ttl.checker.interval=10m
```

请参考[配置页]({{ '/cn/operation/Configuration.html' | relativize_url }})
CN以获取有关设置Alluxio配置的更多详细信息。

#### API

有两种设置路径的TTL属性的方法。

1. 通过Alluxio shell命令行
1. 每个元数据加载或文件创建被动设置

TTL API如下:

```
SetTTL(path，duration，action)
`path` 		Alluxio命名空间中的路径
`duration`	TTL动作生效前的毫秒数，这会覆盖任何先前的设置
`action`	生存时间过去后要执行的`action`。 `FREE`将导致文件
            从Alluxio存储中删除释放，无论其目前的状态如何。 `DELETE`将导致
            文件从Alluxio命名空间和底层存储中删除。
            注意:`DELETE`是某些命令的默认设置，它将导致文件被
            永久删除。
```

#### 命令行用法

了解如何使用`setTtl`命令在Alluxio shell中修改TTL属性参阅详细的
[命令行文档]({{ '/cn/operation/User-CLI.html' | relativize_url }}#setttl)。

#### Alluxio中文件上的被动TTL设置

Alluxio客户端可以配置为只要在Alluxio命名空间添加新文件时就添加TTL属性。
当预期用户是临时使用文件情况下，被动TTL很有用
，但它不灵活，因为来自同一客户端的所有请求将继承
相同的TTL属性。

被动TTL通过以下选项配置:

* `alluxio.user.file.create.ttl`-在Alluxio中文件上设置的TTL持续时间。
默认情况下，未设置TTL持续时间。
* `alluxio.user.file.create.ttl.action`-对文件设置的TTL到期后的操作
在Alluxio中。**注意：默认情况下，此操作为“DELETE”，它将导致文件永久被删除。**

TTL默认情况下处于不使用状态，仅当客户有严格数据访问模式才启用。

例如，要3分钟后删除由`runTests`创建的文件:

```console
$ ./bin/alluxio runTests -Dalluxio.user.file.create.ttl=3m \
  -Dalluxio.user.file.create.ttl.action=DELETE
```

对于这个例子，确保`alluxio.master.ttl.checker.interval`被设定为短
间隔，例如一分钟，以便master能快速识别过期文件。

## 在Alluxio中管理数据复制

### 被动复制

与许多分布式文件系统一样，Alluxio中的每个文件都包含一个或多个分布在集群中存储的存储块。默认情况下，Alluxio可以根据工作负载和存储容量自动调整不同块的复制级别。例如，当更多的客户以类型`CACHE`或`CACHE_PROMOTE`请求来读取此块时Alluxio可能会创建此特定块更多副本。当较少使用现有副本时，Alluxio可能会删除一些不常用现有副本
来为经常访问的数据征回空间([块注释策略](#block-annotation-policies))。
在同一文件中不同的块可能根据访问频率不同而具有不同数量副本。

默认情况下，此复制或征回决定以及相应的数据传输
对访问存储在Alluxio中数据的用户和应用程序完全透明。

### 主动复制

除了动态复制调整之外，Alluxio还提供API和命令行
界面供用户明确设置文件的复制级别目标范围。
尤其是，用户可以在Alluxio中为文件配置以下两个属性:

1. `alluxio.user.file.replication.min`是此文件的最小副本数。
默认值为0，即在默认情况下，Alluxio可能会在文件变冷后从Alluxio管理空间完全删除该文件。
通过将此属性设置为正整数，Alluxio
将定期检查此文件中所有块的复制级别。当某些块
的复制数不足时，Alluxio不会删除这些块中的任何一个，而是主动创建更多
副本以恢复其复制级别。
1. `alluxio.user.file.replication.max`是最大副本数。一旦文件该属性
设置为正整数，Alluxio将检查复制级别并删除多余的
副本。将此属性设置为-1为不设上限(默认情况)，设置为0以防止
在Alluxio中存储此文件的任何数据。注意，`alluxio.user.file.replication.max`的值
必须不少于`alluxio.user.file.replication.min`。

例如，用户可以最初使用至少两个副本将本地文件`/path/to/file`复制到Alluxio:

```console
$ ./bin/alluxio fs -Dalluxio.user.file.replication.min=2 \
copyFromLocal /path/to/file /file
```

接下来，设置`/file`的复制级别区间为3到5。需要注意的是，在后台进程中完成新的复制级别范围设定后此命令将马上返回，实现复制目标是异步完成的。

```console
$ ./bin/alluxio fs setReplication --min 3 --max 5 /file
```

设置`alluxio.user.file.replication.max`为无上限。

```console
$ ./bin/alluxio fs setReplication --max -1 /file
```

重复递归复制目录`/dir`下所有文件复制级别(包括其子目录）使用`-R`:

```console
$ ./bin/alluxio fs setReplication --min 3 --max -5 -R /dir
```

要检查的文件的目标复制水平，运行

```console
$ ./bin/alluxio fs stat /foo
```

并在输出中查找`replicationMin`和`replicationMax`字段。

## 检查Alluxio缓存容量和使用情况

Alluxio shell命令`fsadmin report`提供可用空间的简短摘要
以及其他有用的信息。输出示例如下:

```console
$ ./bin/alluxio fsadmin report
Alluxio cluster summary:
    Master Address: localhost/127.0.0.1:19998
    Web Port: 19999
    Rpc Port: 19998
    Started: 09-28-2018 12:52:09:486
    Uptime: 0 day(s), 0 hour(s), 0 minute(s), and 26 second(s)
    Version: 2.0.0
    Safe Mode: true
    Zookeeper Enabled: false
    Live Workers: 1
    Lost Workers: 0
    Total Capacity: 10.67GB
        Tier: MEM  Size: 10.67GB
    Used Capacity: 0B
        Tier: MEM  Size: 0B
    Free Capacity: 10.67GB
```

Alluxio shell还允许用户检查Alluxio缓存中多少空间可用和在用。

获得Alluxio缓存总使用字节数运行:

```console
$ ./bin/alluxio fs getUsedBytes
```

获得Alluxio缓存以字节为单位的总容量

```console
$ ./bin/alluxio fs getCapacityBytes
```

Alluxio master web界面为用户提供了集群的可视化总览包括已用多少存储空间。可以在`http:/{MASTER_IP}:${alluxio.master.web.port}/`中找到。
有关Alluxio Web界面的更多详细信息可以在
[相关文档]({{ '/en/operation/Web-Interface.html' | relativize_url }}) 中找到。
