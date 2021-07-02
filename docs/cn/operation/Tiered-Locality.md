---
layout: global
title: 层级化的本地性
nickname: 层级化的本地性
group: Operations
priority: 7
---

* Table of Contents
{:toc}

# 层级化的标识

在Alluxio中，所有的组件（masters，workers，clients）都有一个*层级标识*。层级标识是一个地址的组合，例如`(node=...，rack = ...)`。该组合中的每一个元素被称为一个*本地性层级（locality tier）*。本地性层级由最具体到最不具体进行排列，因此若如果两个组件的节点名称相同，则假定它们也在相同的机架内。 Alluxio使用层级标识来优化数据本地性。例如，当客户端想要从UFS读取文件时，它会倾向于借助同一节点上的Alluxio worker进行读取。
如果该节点上没有worker，则将检查下一层级（同机架）优先进行同机架内的数据传输。如果没有worker与客户端在一个节点上或同一机架内，将选择一个任意的worker。

# 配置

如果用户没有提供任何层级的标识信息，每个组件将会查询本地主机信息来设置其节点级的标识。如果其他层级没有设置，它们将不会被用来影响本地性的决定。用户可以通过设置配置属性来设置局部性层的值

```
alluxio.locality.[tiername]=...
```

请参阅[配置文档]({{ '/cn/operation/Configuration.html' | relativize_url }})页面以获取详细信息来设置配置属性。

也可以通过脚本配置层级标识。默认情况下Alluxio在classpath下搜索名为`alluxio-locality.sh`脚本。你可以通过设置指定其他位置的脚本

```
alluxio.locality.script=/path/to/script
```

该脚本需要是可执行的，并输出一个以逗号分隔`tierName=...`的键值对列表。以下是一个可供参考的示例脚本：

```bash
#!/bin/bash

echo "host=$(hostname),rack=/rack1"
```

如果`alluxio.locality.script`中没有脚本，则该属性将被忽略。 如果该脚本返回一个非零返回值或者是格式错误的输出，Alluxio会抛出一个错误。

## 节点位置优先级顺序

有多种方法可以配置节点级本地性。这是从最高优先级到最低优先级的顺序：

1. 设置`alluxio.locality.node`
1. 在由`alluxio.locality.script`配置的脚本的输出中设置`node = ...`
1. 在worker上设置`alluxio.worker.hostname`，在master上设置`alluxio.master.hostname`，或者在客户端上设置`alluxio.user.hostname`。
1. 如果以上都未配置，则节点位置由本地主机查找确定。

# 什么时候层级化的本地性标识会被使用？

1. 当客户端选择一个worker来读取UFS时。
1. 当客户端发现多个Alluxio worker都拥有同一个数据块，来选择一个worker进行读取时。
1. 如果使用`LocalFirstPolicy`或`LocalFirstAvoidEvictionPolicy`的策略向Alluxio写入数据时，选择要写入的worker。

# 自定义本地性层级

默认情况下，Alluxio有两个本地性层级：`node`和`rack`。用户可以自定义一组本次性层级来表达更高级的拓扑结构。要更改可用层级集合，请设置`alluxio.locality.order`。顺序应该从最具体到最不具体。例如，要将可用区域添加到集群，设置

```
alluxio.locality.order=node,rack,availability_zone
```

请注意，必须为包括Alluxio客户端在内的所有组件设置此配置。

要为每个个体设置可用区域，可以设置`alluxio.locality.availability_zone`属性键，或者使用一个包含`availability_zone = ...`输出的本地脚本。

