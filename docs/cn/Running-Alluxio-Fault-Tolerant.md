---
layout: global
title: 在集群上运行Alluxio实现高可用性
nickname: Alluxio集群模式实现高可用性
group: Deploying Alluxio
priority: 3
---

* 内容列表
{:toc}

Alluxio的高可用性通过多master实现。同一时刻，有多个master进程运行。其中一个被选举为leader，作为所有worker和
client的通信首选。其余master进入备用状态，和leader共享日志，以确保和leader维护着同样的文件系统元数据并在
leader失效时迅速接管leader的工作。

当前leader失效时，自动从可用的备用master中选举一个作为新的leader，Alluxio继续正常运行。但在切换到备用
master时，客户端会有短暂的延迟或瞬态错误。

## 前期准备

搭建一个高可用性的Alluxio集群需要两方面的准备：

* [ZooKeeper](http://zookeeper.apache.org/)
* 用于存放日志的可靠的共享底层文件系统。

Alluxio使用Zookeeper实现leader选举，可以保证在任何时间最多只有一个leader。

Alluxio使用共享底层文件系统存放日志。共享文件系统必须可以被所有master访问，可以选择
[HDFS](Configuring-Alluxio-with-HDFS.html), [Amazon S3](Configuring-Alluxio-with-S3.html)或
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html)作为共享文件系统。leader master将日志写到共享文件
系统，其它(备用) master持续地重播日志条目与leader的最新状态保持一致。

### ZooKeeper

Alluxio使用Zookeeper实现master的高可用性。Alluxio master使用Zookeeper选举leader。Alluxio client使用
Zookeeper查询当前leader的id和地址。

ZooKeeper必须单独安装
(见[ZooKeeper快速入门](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

部署Zookeeper之后，记下其地址和端口，下面配置Alluxio时会用到。

### 存放日志的共享文件系统

Alluxio使用共享文件系统存放日志。所有master必须能够从共享文件系统进行读写。只有leader master可以在任何时
间写入日志，但所有master可以读取共享日志来重播Alluxio的系统状态。

共享文件系统必须单独安装（不能通过Alluxio），并且要在Alluxio启动之前处于运行状态。

举个例子，如果使用HDFS共享日志，记下NameNode的地址和端口，下面配置Alluxio时会用到。

## 配置Alluxio
Zookeeper和共享文件系统都正常运行时，需要在每个主机上配置好`alluxio-site.properties`。

### 外部可见地址

“外部可见地址”仅仅是机器上配置的接口地址，对Alluxio集群中其它节点可见。在EC2上，使用`ip-x-x-x-x`地址。而
且不能使用`localhost`或`127.0.0.1`，否则其它节点无法访问该结点。

### 配置容错的Alluxio

实现Alluxio上的高可用性，需要为Alluxio master、worker和client添加额外的配置。在`conf/alluxio-site.properties`中，以
下java选项需要设置：

<table class="table">
<tr><th>属性名</th><th>属性值</th><th>含义</th></tr>
{% for item in site.data.table.java-options-for-fault-tolerance %}
<tr>
  <td>{{item.PropertyName}}</td>
  <td>{{item.Value}}</td>
  <td>{{site.data.table.cn.java-options-for-fault-tolerance[item.PropertyName]}}</td>
</tr>
{% endfor %}
</table>

设置这些选项，可以在`ALLUXIO_JAVA_OPTS`包含：

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

如果集群有多个ZooKeeper节点，指定多个地址时用逗号分割：

    -Dalluxio.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

你也可以选择在`alluxio-site.properties`文件中配置以上的选项。更多配置参数选项请参考[配置设置](Configuration-Settings.html)。

### Master配置

除了以上配置，Alluxio master需要额外的配置，以下变量需在`conf/alluxio-site.properties`中正确设置：

   alluxio.master.hostname=[externally visible address of this machine]

同样，指定正确的日志文件夹需在`conf/alluxio-site.properties`中设置`alluxio.master.journal.folder`，举例而言，如果
使用HDFS来存放日志，可以添加：

    -Dalluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/alluxio/journal

所有Alluxio master以这种方式配置后，都可以启动用于Alluxio的高可用性。其中一个成为leader，其余重播日志直到当
前master失效。

### Worker配置

只要以上参数配置正确，worker就可以咨询ZooKeeper，找到当前应当连接的master。所以，worker无需设置`alluxio.master.hostname`。

> 注意: 当在高可用性模式下运行Alluxio, worker的默认心跳超时时间可能太短。
> 为了能在master进行故障转移时正确处理master的状态，建议将worker的默认心跳超时时间设置的长点。
> 增加worker上的默认超时时间，可以通过修改`conf/alluxio-site.properties`下的配置参数
> `alluxio.worker.block.heartbeat.timeout.ms` 至一个大些的值（至少几分钟）。

### Client配置

无需为高可用性模式配置更多的参数，只要以下两项：

    -Dalluxio.zookeeper.enabled=true
    -Dalluxio.zookeeper.address=[zookeeper_hostname]:2181

在client应用中正确设置，应用可以咨询ZooKeeper获取当前 leader master。

#### HDFS API

如果使用HDFS API与高可用性模式的Alluxio通信，使用`alluxio-ft://`模式来代替`alluxio://`。在URL中的所有主机名都将被忽略，相应地，`alluxio.zookeeper.address`配置会被读取，从而寻找Alluxio leader master。

```bash
hadoop fs -ls alluxio-ft:///directory
```
