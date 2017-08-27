---
layout: global
title: 在集群上独立运行Alluxio
nickname: 在集群上独立运行Alluxio
group: Deploying Alluxio
priority: 2
---

* 内容列表
{:toc}

## 下载Alluxio

下载`Alluxio` tar文件并解压：

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

## 配置Alluxio

在`${ALLUXIO_HOME}/conf`目录下，从模板创建`conf/alluxio-site.properties`配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

更新`conf/alluxio-site.properties`中的`alluxio.master.hostname`为你将运行Alluxio Master的机器的主机名。添加所有worker节点的IP地址到`conf/workers`文件。最后，同步所有信息到worker节点。你可以使用

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

来同步文件和文件夹到所有的`alluxio/conf/workers`中指定的主机。

## EC2集群上使用Spark
如果使用Spark启动EC2集群，`Alluxio`会默认被安装和配置。
