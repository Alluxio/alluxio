---
layout: global
title: Data Caching
---

### Alluxio存储概述
本文档的目的是向用户介绍 Alluxio 存储背后的概念以及可以在 Alluxio 存储空间中执行的操作。

Alluxio 有助于跨各种平台统一用户数据，同时还有助于提高整体 I/O 吞吐量。 Alluxio 通过将存储分为两个不同的类别来实现这一目标。

- **UFS（Under File Storage，也称为底层存储）**
     - 这种类型的存储表示为不受 Alluxio 管理的空间。 UFS 存储可能来自外部文件系统，包括 HDFS 或 S3。 Alluxio 可以连接到一个或多个 UFS 并将它们暴露在单个命名空间中。
     - 通常，UFS 存储旨在长时间持久化存储大量数据。
- **Alluxio存储**
     - Alluxio 管理 Alluxio Worker, 包括内存的本地存储，充当分布式缓存。这种位于用户应用程序和各种底层存储之间的快速数据层可大大提高 I/O 性能。
     - Alluxio存储主要为了存储瞬态热数据，并不注重长期持久性。
     - 每个Alluxio worker节点管理的存储量和类型由用户配置决定。
     - 即使数据当前不在 Alluxio 存储中，其关联的 UFS 中的文件仍然对 Alluxio 客户端可见。当客户端尝试读取只存在于 UFS 中的文件时，数据会被复制到 Alluxio 存储中。


![Alluxio storage diagram]({{ '/img/stack.png' | relativize_url }})

![Alluxio storage diagram]({{ '/img/stack.png' | relativize_url }})
Alluxio 存储通过将数据存储在与计算节点位于同一位置的内存中来提高性能。 Alluxio 存储中的数据可以被复制，以使“热”数据更容易供 I/O 操作并行使用。

Alluxio 中的数据副本和 UFS 中可能存在的副本是互相独立存在的。 Alluxio 存储中的数据副本数量由集群活动动态决定。 由于Alluxio依赖于底层文件存储来存储大部分数据，因此Alluxio不需要保留未使用的数据的副本。

Alluxio还支持分层存储配置，例如内存、SSD和HDD层，可感知存储系统介质。 这可以减少拉取数据的延迟，类似于 L1/L2 CPU 缓存的操作方式。

## Dora缓存概述
Dora 项目中 Alluxio 元数据和缓存管理的主要变化在于不再由单个master 节点负责所有文件系统的元数据和缓存信息。“worker 节点”，或者简称为“Dora 缓存节点”，现在也会处理文件的元数据和数据。 客户端只需向 Dora 缓存节点发送请求，每个 Dora 缓存节点将同时提供所请求文件的元数据和数据。 由于有大量服务中的Dora缓存节点，所以客户端流量不必流向单个master节点，而是分布在多个Dora缓存节点之间，从而大大减轻了master节点的负担。

### Dora节点之间的负载均衡
如果没有master节点指示客户端去哪些worker节点拉取数据，那么客户端需要通过其他方法来选择其目标节点。 Dora 采用一种简单而有效的算法（称为[一致性哈希](https://en.wikipedia.org/wiki/Consistent_hashing)）来确定性地计算目标节点。 一致性哈希确保就给定的所有可用 Dora 缓存节点列表，对于特定请求的文件，任何客户端都将独立选择同一节点来请求该文件。 如果使用的哈希算法在节点上是[统一的](https://en.wikipedia.org/wiki/Hash_function#Uniformity)，则请求将在节点之间均匀分布（对请求的分布取模）。

一致性哈希允许 Dora 集群随着数据集的大小线性扩展，因为所有元数据和数据都被分区到多个节点上，不会有任何一个节点出现单点故障。

### 容错和客户端 UFS 回退兜底

Dora 缓存节点有时会遇到严重问题并停止服务。为了确保即使某个节点出现故障，客户端的请求也能正常得到服务，有一种回退机制支持客户端从故障节点回退到另一个节点，最终如果所有可能的选项都用尽，则回退到 UFS 进行兜底。

对于一个给定的文件，一致性哈希算法选择的目标节点是处理有关该文件的请求的首选节点。 一致性哈希允许客户端在首选节点发生故障后计算出次选节点，并将请求重定向到次选节点。 与首选节点一样，不同客户端独立计算的次选节点完全相同，保证回退发生在同一个节点上。 这个回退过程可以发生多次（可由用户配置次数），直到重试多个节点的成本变得不可接受，此时客户端可以直接回退到 UFS 进行兜底。

### 元数据管理
Dora 缓存节点缓存它们所负责的文件的元数据。 当客户端第一次请求文件时，会直接从 UFS 中获取元数据，然后由 Dora 节点缓存。 之后，Dora节点就能被用来响应后续的元数据请求。

目前，Dora 架构仅面向不可变和只读用例。 这假设 UFS 中文件的元数据不会随时间变化，因此不必使缓存的元数据失效。 将来，我们希望探索某些需要使元数据失效但相对较少的用例。

## 配置Alluxio存储

### 分页存储

* ▶️ [什么是分页存储？](https://youtu.be/qFF7qef__OY){:target="_blank"} (1:06)
* ▶️ [分页存储的好处](https://www.youtube.com/watch?v=cO7ymRFnPyM){:target="_blank"} (2:44)
* ▶️ [使用分页存储的高效 Alluxio 缓存](https://youtu.be/7UY_iE_Ha_k){:target="_blank"} (1:18)

Alluxio 在 Alluxio Worker 上支持更细粒度的页面级（通常为 1 MB）缓存存储，作为现有基于块（默认为 64 MB）分层缓存存储的替代选项。 该分页存储支持包括读写在内的一般工作负载，并具有类似于分层块存储中的[块注释策略]({{ '/cn/core-services/Data-Caching.html' | relativize_url }}#block-annotation-policies)的可自定义缓存驱逐策略。

切换到分页存储：
```properties
alluxio.worker.block.store.type=PAGE
```
您可以指定多个目录用作分页存储的缓存存储。例如，使用两个 SSD（分别挂载在`/mnt/ssd1`和`/mnt/ssd2`）：

```properties
alluxio.worker.page.store.dirs=/mnt/ssd1,/mnt/ssd2
```

您可以为每个目录设置缓存最大存储量的限制。 例如，要在每个 SSD 上分配 100 GB 空间：

```properties
alluxio.worker.page.store.sizes=100GB,100GB
```
请注意，存储量大小的顺序必须与目录的顺序一致。由于分配器（Allocator) 会均匀分配数据，因此强烈建议将所有目录分配为相同大小。

指定页面大小：
```properties
alluxio.worker.page.store.page.size=1MB
```
较大的页面大小可能会提高顺序读取性能，但可能会占用更多的缓存空间。 我们建议对 Presto 工作负载（读取 Parquet 或 Orc 文件）使用默认值 (1MB)。

启用分页存储的异步写入：
```properties
alluxio.worker.page.store.async.write.enabled=true
```
如果您发现存在大量缓存未命中导致性能下降，设置此属性会很有帮助。
