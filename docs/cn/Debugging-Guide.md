---
layout: global
title: 调试指南
group: Resources
---

* Table of Contents
{:toc}

本页面主要是关于Alluxio使用过程中的一些指导和提示，方便用户能够更快的解决使用过程遇到的问题。

注意: 本页面的定位并不是指导解决Alluxio使用过程中遇到的所有问题。
用户可随时向[Alluxio邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)或其[镜像](http://alluxio-users.85194.x6.nabble.com/)提交问题。

## Alluxio日志地址

Alluxio运行过程中可产生master、worker和client日志，这些日志存储在`{ALLUXIO_HOME}/logs`文件夹中，日志名称分别为
`master.log`,`master.out`, `worker.log`, `worker.out` 和`user.log`。

master和worker日志对于理解Alluxio master节点和worker节点的运行过程是非常有帮助的，当Alluxio运行出现问题时，可以查阅日志发现问题产生原因。如果不清楚错误日志信息，可在[邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)查找，错误日志信息有可能之前已经讨论过。

## Alluxio部署常见问题

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
- 如果底层文件存储系统是S3，检查 `ufs.yml`文件中的S3 bucket名称是否为已存在的bucket的名称，不包括`s3://` 、`s3a://`或者`s3n://`前缀
- 如果不能访问UI，检查安全组是否限制了端口19999

## ALLuxio存储常见问题

#### 问题: 出现类似如下的错误信息: "Frame size (67108864) larger than max length (16777216)",这种类型错误信息出现的原因是什么?

解决办法: 多种可能的原因会导致这种错误信息的出现。

- 请仔细检查Alluxio的master节点的端口(port)是否正确，Alluxio的master节点默认的监听端口号为19998。
通常情况下master地址的端口号错误会导致这种错误提示的出现(例如端口号写成了19999,而19999是Alluxio的master节点的web用户界面的端口号)
- 请确保Alluxio的master节点和client节点的安全设置保持一致.
Alluxio通过配置`alluxio.security.authentication.type`来提供不同的用户身份验证(Security.html#authentication)的方法。
如果客户端和服务器的这项配置属性不一致，这种错误将会发生。(例如，客户端的属性为默认值`NOSASL`,而服务器端设为`SIMPLE`)
有关如何设定Alluxio的集群和应用的问题，用户请参照[Configuration-Settings](Configuration-Settings.html)

####　向Alluxio拷贝数据或者写数据时出现如下问题 "Failed to cache: Not enough space to store block on worker",为什么？

解决办法: 这种错误说明alluxio空间不足，无法完成用户写请求。

- 如果你使用`copyFromLocal`命令向Alluxio写数据，shell命令默认使用`LocalFirstPolicy`命令,并将数据存储到本地worker节点上(查看[location policy](File-System-API.html#location-policy))
如果本地worker节点没有足够空间，你将会看到上述错误。
你可以通过将策略修改为`RoundRobinPolicy`(如下所述)来将你的文件分散存储到不同worker节点上。

```bash
$ bin/alluxio fs -Dalluxio.user.file.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy copyFromLocal foo /alluxio/path/foo
```

- 检查一下内存中是否有多余的文件并从内存中释放这些文件。查看[Command-Line-Interface](Command-Line-Interface.html)获取更多信息。
- 通过改变`alluxio.worker.memory.size`属性值增加worker节点可用内存的容量，查看[Configuration](Configuration-Settings.html#common-configuration) 获取更多信息。

## Alluxio性能常见问题

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
