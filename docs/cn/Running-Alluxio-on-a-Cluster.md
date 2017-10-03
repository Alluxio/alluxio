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

下载Alluxio tar文件并解压：

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

## 配置Alluxio

在`${ALLUXIO_HOME}/conf`目录下，从模板创建`conf/alluxio-site.properties`配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

更新`conf/alluxio-site.properties`中的`alluxio.master.hostname`为你将运行Alluxio Master的机器的主机名。添加所有worker节点的IP地址到`conf/workers`文件。
如果集群中存在多节点，你不可以使用本地文件系统作为Allxuio底层存储层。你需要在所有Alluxio服务端连接的节点启动共享存储，共享存储可以是
网络文件系统（NFS)，HDFS，S3等。例如。你可以参照[Configuring Alluxio with S3](Configuring-Alluxio-with-S3.html)按照说明启动S3作为Alluxio底层存储。

最后，同步所有信息到worker节点。你可以使用

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

来同步文件和文件夹到所有的`alluxio/conf/workers`中指定的主机。如果你只在Alluxio master节点上下载并解压了Alluxio压缩包，你可以使用`copyDir`命令同步worker节点下的Alluxio文件夹，你同样可以
使用此命令同步`conf/alluxio-site.properties`中的变化到所有worker节点。

## 启动 Alluxio

现在，你可以启动 Alluxio:

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

为了确保Alluxio正在运行, 访问 `http://<alluxio_master_hostname>:19999`, 检查文件夹`alluxio/logs`下的日志, or 或者运行简单程序:

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**注意**: 如果你使用EC2, 确保master节点上的安全组设置允许来自alluxio web UI 端口的连接。
