---
layout: global
title: Tiered Locality
nickname: Tiered Locality
group: Features
priority: 1
---

* Table of Contents
{:toc}

＃分层标识

在Alluxio中，每个实体（masters，workers，clients）都有一个*分层标识*。分层标识是一个地址的元组，例如`（node = ...，rack = ...）`。元组中的每一对被称为一个*locality tier*。locality tier从最具体到最不具体进行排列，所以如果两个实体具有相同的节点名称，则假定它们也具有相同的机架名称。 Alluxio使用分层标识优化本地性。例如，当客户端想要从UFS读取文件时，它会倾向于从同一节点上的Alluxio worker进行读取。
如果第一层（节点）中没有本地worker，则将检查下一层（机架）以优先进行机架本地数据传输。如果没有worker与客户端共享一个节点或机架，将选择一个任意的worker。

＃配置

如果用户没有提供任何分层的标识信息，每个实体将会执行本地主机查找来设置其节点级别的标识信息。如果其他locality tier没有设置，他们将不会被用来通知locality的决定。可以通过设置配置属性来明确配置locality tier的值

```
alluxio.locality。[tiername] = ...
```

请参阅[配置-设置]（Configuration-Settings.html）页面以获取详细信息来设置配置属性。

也可以通过脚本配置分层的标识信息。要使用脚本，请设置

```
alluxio.locality.script=/path/to/script
```

该脚本应该是可执行的，并输出`tierName = ...`列表对，以逗号分隔。以下是供参考的示例脚本：

```bash
#!/bin/bash

echo "host=$(hostname),rack=/rack1"
```

##节点位置优先级顺序

配置节点位置的方法有很多。这是从最高优先级到最低优先级的顺序：

1. 设置`alluxio.locality.node`
1. 在由`alluxio.locality.script`配置的脚本的输出中设置`node = ...`
1. 在worker上设置`alluxio.worker.hostname`，在master上设置`alluxio.master.hostname`，或者在客户端上设置`alluxio.user.hostname`。
1. 如果以上都未配置，则节点位置由本地主机查找确定。

＃何时使用tiered locality？

1. 当选择一个worker从UFS读取时。
1. 当多个Alluxio worker拥有一个块，选择一个worker进行读取时。
1. 如果使用`LocalFirstPolicy`或`LocalFirstAvoidEvictionPolicy`，tiered locality是
用于在向Alluxio写入数据时选择要写入的worker。

＃自定义locality tiers

默认情况下，Alluxio有两个locality tiers：`node`和`rack`。用户可以自定义一组locality tiers来利用更高级的拓扑。要更改可用层集，请设置`alluxio.locality.order`。顺序应该从最具体到最不具体。例如，要将可用区域位置添加到集群，请进行设置

```
alluxio.locality.order=node,rack,availability_zone
```

请注意，必须为所有实体（包括Alluxio客户端）设置此配置。

现在为每个实体设置可用区域，设置`alluxio.locality.availability_zone`属性键，或者使用一个包含`availability_zone = ...`输出的本地脚本。

