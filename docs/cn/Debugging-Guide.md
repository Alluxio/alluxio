---
layout: global
title: 调试指南
group: Resources
---

* Table of Contents
{:toc}

本页主要是关于Alluxio使用过程中的一些指导和提示，方便用户能够更快的解决使用过程遇到的问题。
注意: 本页并不包含Alluxio使用过程中遇到的所有问题。
用户可随时向 [Alluxio邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)提交问题。

## Alluxio日志地址

Alluxio运行过程中可产生master、worker和client日志，这些日志存储在`{ALLUXIO_HOME}/logs`文件夹中，日志名称分别为
`master.log`,`master.out`, `worker.log`, `worker.out` 和`user.log`。

master和worker日志对于理解Alluxio master节点和worker节点的运行过程是非常有帮助的，当Alluxio运行出现问题时，可以查阅日志发现问题产生原因。如果不清楚错误日志信息，可在[邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)查找，错误日志信息有可能之前已经讨论过。

## Alluxio安装常见问题解答

#### 问题: 在本地机器上初次安装使用Alluxio失败，应该怎么办？

解决办法: 首先检查目录`{ALLUXIO_HOME}/logs`下是否存在master和worker日志，然后按照日志提示的错误信息进行操作。否则，再次检查是否遗漏了[本地运行Alluxio](Running-Alluxio-Locally.html)里的配置步骤

典型问题:

- `ALLUXIO_UNDERFS_ADDRESS`配置不正确
- 如果 `ssh localhost` 失败, 请确认`~/.ssh/authorized_keys`文件中包含主机的ssh公钥

#### 问题: 打算在Spark/HDFS集群中部署Alluxio，有什么建议？

解决办法: 按照[集群环境运行Alluxio](Running-Alluxio-on-a-Cluster.html), 
 [Alluxio配置HDFS](Configuring-Alluxio-with-HDFS.html)提示操作。

提示:

- 通常情况下, 当Alluxio workers和计算框架的节点部署在一起的时候，性能可达到最优
- 如果你正在使用Mesos或者Yarn管理集群,也可以将Mesos和Yarn集成到Alluxio中，使用Mesos和Yarn可方便集群管理
- 如果底层存储是远程的，比如说S3或者远程HDFS,这种情况下，使用Alluxio会非常有帮助

#### 问题: 在EC2上安装Alluxio遇到问题，有什么建议？

解决办法: 可按照[EC2上运行Alluxio](Running-Alluxio-on-EC2.html)提示操作。

典型问题:

- 请确定 AWS access keys 和 Key Pairs 已安装
- 如果底层文件存储系统是S3，检查 `ufs.yml`文件中的S3 bucket名称是否为已存在的bucket的名称，不包括`s3://` 或者`s3n://`前缀
- 如果不能访问UI，检查安全组是否限制了端口19999

## Alluxio性能常见问题解答

#### 问题: 在Alluxio/Spark上进行测试（对大小为GBs的文件运行单词统计），相对于HDFS/Spark，性能并无明显差异。为什么?

解决办法: Alluxio通过使用分布式的内存存储（以及分层存储）和时间或空间的本地化来实现性能加速。如果数据集没有任何本地化, 性能加速效果并不明显。 

## 环境

Alluxio在不同的生产环境下可配置不同的运行模式。
请确定当前Alluxio版本是最新的并且是支持的版本。

在[邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)提交问题时请附上完整的环境信息,包括

- Alluxio版本
- 操作系统版本
- Java版本
- 底层文件系统类型和版本
- 计算框架类型和版本
- 集群信息, 如节点个数, 每个节点内存, 数据中心内部还是跨数据中心运行