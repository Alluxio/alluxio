---
layout: global
title: 分层局部性
nickname: 分层局部性
group: Features
priority: 1
---

* Table of Contents
{:toc}

＃分层标识

在Alluxio中，每个实体（masters，workers，clients）都有一个*分层标识*。分层标识是一个地址的元组，例如`（node = ...，rack = ...）`。元组中的每一对被称为一个*局部性层（locality tier）*。局部性层从最具体到最不具体进行排列，所以如果两个实体具有相同的节点名称，则假定它们也具有相同的机架名称。 Alluxio使用分层标识优化本地性。例如，当客户端想要从UFS读取文件时，它会倾向于从同一节点上的Alluxio worker进行读取。
如果第一层（节点）中没有本地worker，则将检查下一层（机架）以优先进行机架本地数据传输。如果没有worker与客户端共享一个节点或机架，将选择一个任意的worker。

＃配置

如果用户没有提供任何分层的标识信息，每个实体将会执行本地主机查找来设置其节点级别的标识信息。如果其他局部性层没有设置，他们将不会被用来影响局部性的决定。可以通过设置配置属性来设置局部性层的值

```
alluxio.locality.[tiername] = ...
```

请参阅[配置项设置](Configuration-Settings.html)页面以获取详细信息来设置配置属性。

也可以通过脚本配置分层的标识信息。默认情况下Alluxio在`${ALLUXIO_HOME}/conf/tiered_identity.sh`中寻找脚本。你可以通过设置覆盖这个位置

```
alluxio.locality.script=/path/to/script
```

该脚本应该是可执行的，并输出`tierName = ...`列表对，以逗号分隔。以下是供参考的示例脚本：

```bash
#!/bin/bash

echo "host=$(hostname),rack=/rack1"
```

如果`alluxio.locality.script`中没有脚本，则该属性将被忽略。 如果该脚本返回一个非零返回值或者是格式错误的输出，Alluxio会发生错误。

##节点位置优先级顺序

有多种配置节点位置的方法。这是从最高优先级到最低优先级的顺序：

1. 设置`alluxio.locality.node`
1. 在由`alluxio.locality.script`配置的脚本的输出中设置`node = ...`
1. 在worker上设置`alluxio.worker.hostname`，在master上设置`alluxio.master.hostname`，或者在客户端上设置`alluxio.user.hostname`。
1. 如果以上都未配置，则节点位置由本地主机查找确定。

＃什么时候分层局部性会被使用？

1. 当选择一个worker从UFS读取时。
1. 当多个Alluxio worker拥有一个块，选择一个worker进行读取时。
1. 如果使用`LocalFirstPolicy`或`LocalFirstAvoidEvictionPolicy`，分层局部性被用于在向Alluxio写入数据时选择要写入的worker。

＃自定义局部性层

默认情况下，Alluxio有两个局部性层：`node`和`rack`。用户可以自定义一组局部性层来利用更高级的拓扑。要更改可用层集，请设置`alluxio.locality.order`。顺序应该从最具体到最不具体。例如，要将可用区域位置添加到集群，请进行设置

```
alluxio.locality.order=node,rack,availability_zone
```

请注意，必须为所有实体（包括Alluxio客户端）设置此配置。

要为每个实体设置可用区域，可以设置`alluxio.locality.availability_zone`属性键，或者使用一个包含`availability_zone = ...`输出的本地脚本。

