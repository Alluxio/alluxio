---
布局：全局
标题：术语表
---

Alluxio
：Alluxio是一个分布式数据编排系统，可提供统一的接口来访问各种不同存储系统中的数据，从而实现更强的性能和更好的数据本地性。[了解Alluxio的更多内容]({{ '/en/introduction/Introduction.html' | relativize_url }})

客户端
：客户端是一个与Alluxio系统交互以访问和操作存储在Alluxio中的数据的组件或实体。客户端通常是利用Alluxio作为分布式存储层的应用程序或框架，以实现高效的数据访问和处理。[参见Dora架构中的客户端]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

一致性哈希
：一致性哈希是分布式系统中用于有效地将数据分布在多个节点上的技术，以便在系统中添加或移除节点时减少数据移动和中断。[参见Dora缓存中的一致性哈希]({{ '/en/core-services/Data-Caching.html#load-balancing-between-dora-nodes' | relativize_url }})

数据缓存
：Alluxio提供了一种内存数据缓存机制来加速数据访问。频繁访问的数据被缓存在内存中，减少了重复性的磁盘I/O需求，提高了整体应用程序性能。 [了解更多]({{ '/en/core-services/Data-Caching.html' | relativize_url }})

数据湖
：数据湖是一个集中式仓库，用于存储大量原始的结构化和非结构化数据，并保留其原生格式，使得不同数据源和格式的数据存储与分析变得灵活且可扩展。

容错
：Alluxio包含了可确保数据可用性和可靠性的容错机制。它在集群中的不同节点上复制数据，提供可应对节点故障的弹性。 [参见Dora缓存中的容错]({{ '/en/core-services/Data-Caching.html#fault-tolerance-and-client-side-ufs-fallback' | relativize_url }})

FUSE
：用户空间文件系统（FUSE）是Unix和类Unix操作系统的一个软件接口，允许非特权用户创建自己的文件系统，而无需修改内核代码。 [了解Alluxio FUSE SDK的更多内容]({{ '/en/fuse-sdk/FUSE-SDK-Overview.html' | relativize_url }})

FUSE 内核缓存
：内核缓存是操作系统内核采用的一种缓存机制，用于提高文件系统操作的性能，推荐用于元数据缓存。内核缓存将最近访问的文件系统元数据存储在内存中，降低了访问底层存储设备的需求。 [了解如何配置内核数据缓存]({{ '/en/fuse-sdk/Local-Cache-Tuning.html#local-kernel-data-cache-configuration' | relativize_url }})或[内核元数据缓存]({{ '/en/fuse-sdk/Local-Cache-Tuning.html#local-kernel-metadata-cache-configuration' | relativize_url }})

FUSE 用户空间缓存
：用户空间缓存是一种在应用程序或用户空间级别运行的缓存机制，推荐用于数据缓存。它涉及将经常访问的数据存储在离应用程序更近的地方，通常是在内存中，以提高性能并降低进行昂贵的磁盘或网络操作的需求。用户空间缓存提供了更细粒度的缓存控制（例如，缓存介质、最大缓存大小、驱逐策略），并且缓存不会意外地影响容器化环境中的其他应用程序。[了解如何配置用户空间数据缓存]({{ '/en/fuse-sdk/Local-Cache-Tuning.html#local-userspace-data-cache-configuration' | relativize_url }}) 或 [用户空间元数据缓存]({{ '/en/fuse-sdk/Local-Cache-Tuning.html#local-userspace-metadata-cache-configuration' | relativize_url }})

Helm
：Helm是Kubernetes的开源包管理器。它通过提供模板化引擎和一系列预配置的应用程序包（称为“Charts”）简化在Kubernetes集群上部署和管理应用程序的过程。

：Helm Charts帮助您定义、安装和升级Kubernetes应用程序。请参阅[在Kubernetes上安装Alluxio]({{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.html' | relativize_url }})了解如何在Kubernetes上的Alluxio部署中使用Helm Charts。

作业
：作业指的是由一个应用程序或框架在与作为数据存储层的Alluxio交互时执行的计算或处理任务。

作业服务
：作业服务帮助协调并监控使用Alluxio作为存储层的数据访问或处理作业的执行。

Kubernetes
：Kubernetes（也称为K8s）是一个用于自动化部署、扩展和管理容器化应用程序的开源系统。它将构成应用程序的容器分组成逻辑单元，以便于管理和发现。[在Kubernetes上安装Alluxio]({{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.html' | relativize_url }})

Master
：Alluxio Master处理所有用户请求并记录文件系统的元数据更改。Alluxio Job Master是一个充当文件系统操作的轻量级调度器进程，这些操作随后在Alluxio Job Worker上执行。

：Alluxio Master可以被部署为一个leading master和几个standby master的形式以实现容错。当leading master宕机时，将选举一个standby master成为新的leading master。

元数据管理
：Alluxio管理存储在不同存储系统中的数据相关元数据。它跟踪元数据变化，提供元数据缓存，并确保不同存储系统之间的一致性和连贯性。 [了解元数据缓存和失效的更多内容]({{ '/en/core-services/Metadata-Caching.html' | relativize_url }})

Operator
：Operator是Kubernetes的软件扩展，使用自定义资源来管理应用程序及其组件。请参阅[在Kubernetes上安装Alluxio]({{ '/en/kubernetes/Install-Alluxio-On-Kubernetes.html' | relativize_url }})了解如何在Kubernetes上的Alluxio部署中使用Operator。

Worker节点上的分页存储
：Alluxio支持在其worker节点上实现更细粒度的页面级缓存存储，作为现有基于块的分层缓存存储的替代选项。这种分页存储支持包括读取和写入在内的通用工作负载，并提供可定制的缓存驱逐策略。 [了解更多]({{ '/en/core-services/Data-Caching.html#paging-worker-storage' | relativize_url}})

PrestoDB
：Presto是一个面向大型数据集的开源分布式SQL查询引擎，专为运行跨多个数据源的交互式分析查询而设计。它允许使用类似SQL的语言查询来自多个数据源的数据，包括关系型数据库、NoSQL数据库和文件系统。 [使用Alluxio运行Presto]({{ '/en/compute/Presto.html' | relativize_url }})

调度器
：调度器有助于优化资源利用、任务调度和数据本地性，以确保在分布式计算环境中有效执行任务。它处理所有异步作业，如将数据预加载到worker节点。 [参见Dora架构中的调度器]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

服务注册
：服务注册充当一个中央仓库，允许客户端和其他服务与不同组件或服务实例进行通信。它负责服务发现并维护worker节点的列表。 [参见Dora架构中的服务注册]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})

透明数据访问
：Alluxio允许应用程序访问存储在不同存储系统中的数据，而无需修改应用程序代码。它提供了一个透明的数据访问层，实现与现有应用程序和框架的无缝集成。

Trino
：Trino是一个面向大规模数据集的分布式SQL查询引擎，旨在支持快速交互式SQL查询，支持各种数据源，并提供出色的性能和可扩展性。 [使用Alluxio运行Trino]({{ '/en/compute/Trino.html' | relativize_url }})

底层文件存储 (UFS)
：底层文件存储，也称为底层存储，是一种不受Alluxio管理的存储空间类型。UFS存储可以来自外部文件系统，包括HDFS或S3。[参见数据缓存]({{ '/en/core-services/Data-Caching.html' | relativize_url}})

统一命名空间
：Alluxio提供跨多个存储系统的统一命名空间，创建了数据的逻辑视图。它允许应用程序以一致的方式与数据交互，而无需考虑数据被存储的物理位置。

Worker
：Worker是在Alluxio系统中积极参与存储和管理数据的组件或节点。Worker节点在数据缓存、数据移动和为客户端或计算框架提供数据方面发挥着关键作用。 [参见Dora架构中的Worker节点]({{ '/en/introduction/Introduction.html#dora-architecture' | relativize_url }})
