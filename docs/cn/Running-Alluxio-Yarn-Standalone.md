---
layout: global
title: 在YARN Standalone模式上运行Alluxio
nickname: 在YARN Standalone模式上运行Alluxio
group: Deploying Alluxio
priority: 6
---

Alluxio应与YARN一起运行，以便所有YARN节点都可以访问本地的Alluxio worker。
为了YARN和Alluxio能在同一节点上良好地共同运作，应该让YARN知道Alluxio使用的资源。YARN需要知道多少内存和CPU被分配给了Alluxio。

## 为Alluxio分配资源

### Alluxio Worker内存

Alluxio worker节点请求一些内存用于自己的JVM进程，以及一些内存用于作虚拟内存盘。
给JVM的内存一般1GB就足够了，因为这部分内存只用与缓冲和元数据。如果worker正在为内存加速数据使用虚拟内存盘，分配给虚拟内存盘的内存应该加到1GB的JVM内存上，以获得worker所需的总内存。能被虚拟内存盘使用的内存量由 `alluxio.worker.memory.size` 控制。存储在非内存存储层的数据，如SSD或者HDD上的数据无需包括在内存大小计算中。

### Alluxio Master内存

Alluxio master节点存储Alluxio每个文件的元数据，所以它需要一部分内存。这部分至少1GB，在生产部署中，我们推荐使用至少32GB。

### 虚拟CPU核

对于Alluxio worker节点来说，一个虚拟核就足够了。对于生产部署中的Alluxio master节点，我们推荐至少4个虚拟核。

### YARN配置

为了通知YARN在每个节点上有哪些为Alluxio保留的资源，在 `yarn-site.xml` 中编辑YARN配置参数 `yarn.nodemanager.resource.memory-mb` 和 `yarn.nodemanager.resource.cpu-vcores`。
当确定分配多少内存给Alluxio后，从 `yarn.nodemanager.resource.memory-mb` 中减去这个数，用新的值更新参数。在 `yarn.nodemanager.resource.cpu-vcores` 也一样。

更新YARN配置后，重启YARN以保证更改生效：

```bash
$ ${HADOOP_HOME}/sbin/stop-yarn.sh
$ ${HADOOP_HOME}/sbin/start-yarn.sh
```
