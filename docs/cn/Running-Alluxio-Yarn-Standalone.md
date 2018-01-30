---
layout: global
title: 在YARN Standalone模式上运行Alluxio
nickname: 在YARN Standalone模式上运行Alluxio
group: Deploying Alluxio
priority: 6
---

Alluxio应与YARN一起运行，以便所有YARN节点都可以访问本地的Alluxio worker。
为了YARN和Alluxio能在同一节点上良好地共同运作，必须区分节点上YARN和Alluxio的内存和计算资源。要做到这一点，首先要确定Alluxio需要多少内存和CPU，然后从YARN使用的资源上减去那些资源。

## 内存

在`yarn-site.xml`中设置

```
yarn.nodemanager.resource.memory-mb = Total RAM - Other services RAM - Alluxio RAM
```

在Alluxio worker节点上，Alluxio RAM的使用量是1GB加上虚拟内存的大小，通过`alluxio.workder.memory.size`配置。
在Alluxio master节点上，Alluxio RAM的使用量与文件的数量成正比。分配至少1GB，在生产部署中推荐使用32GB。

## 虚拟CPU核

在`yarn-site.xml`中设置

```
yarn.nodemanager.resource.cpu-vcores = Total cores - Other services vcores - Alluxio vcores
```

在Alluxio worker节点上，Alluxio只需要1或2个虚拟核。
在Alluxio master节点上，我们推荐至少4个虚拟核。

## 重启YARN

更新YARN配置后，重启YARN以保证更改生效：

```bash
$ ${HADOOP_HOME}/sbin/stop-yarn.sh
$ ${HADOOP_HOME}/sbin/start-yarn.sh
```
