---
layout: global
title: Tachyon独立模式实现容错
nickname: Tachyon独立模式实现容错
group: User Guide
priority: 3
---

Tachyon的容错通过多master实现。同一时刻，有多个master进程运行。其中一个被选举为leader，作为所有worker和client的通信首选。其余master进入备用状态，和leader共享日志，以确保和leader维护着同样的文件系统元数据并在leader失效时迅速接管leader的工作。

当前leader失效时，自动从可用的备用master中选举一个作为新的leader，Tachyon继续正常运行。但在切换到备用master时，客户端会有短暂的延迟或瞬态错误。

## 前期准备

搭建一个容错的Tachyon集群需要两方面的准备：

* [ZooKeeper](http://zookeeper.apache.org/)
* 用于存放日志的可靠的共享底层文件系统。

Tachyon使用Zookeeper实现容错和leader选举，可以保证在任何时间最多只有一个leader。

Tachyon使用共享底层文件系统存放日志。共享文件系统必须可以被所有master访问，可以选择[HDFS](Configuring-Tachyon-with-HDFS.html), [Amazon S3](Configuring-Tachyon-with-S3.html)或[GlusterFS](Configuring-Tachyon-with-GlusterFS.html)作为共享文件系统。leader master将日志写到共享文件系统，其它(备用) master持续地重播日志条目与leader最新的状态保持一致。

### ZooKeeper

Tachyon使用Zookeeper实现master的容错。Tachyon master使用Zookeeper选举leader。Tachyon client使用Zookeeper查询当前leader的id和地址。

ZooKeeper必须单独安装
(见[ZooKeeper快速入门](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

部署Zookeeper之后，记下其地址和端口，下面配置Tachyon时会用到。

### 存放日志的共享文件系统

Tachyon使用共享文件系统存放日志。所有master必须能够从共享文件系统进行读写。只有leader master可以在任何时间写入日志，但所有master可以读取共享日志来重播Tachyon的系统状态。

共享文件系统必须单独安装（不能通过Tachyon），并且要在Tachyon启动之前处于运行状态。

举个例子，如果使用HDFS共享日志，记下NameNode的地址和端口，下面配置Tachyon时会用到。

## 配置Tachyon
Zookeeper和共享文件系统都正常运行时，需要在每个主机上配置好`tachyon-env.sh`。

### 外部可见地址

“外部可见地址”仅仅是机器上配置的接口地址，对Tachyon集群中其它节点可见。在EC2上，使用`ip-x-x-x-x`地址。而且不能使用`localhost`或`127.0.0.1`，否则其它节点无法访问该结点。

### 配置容错的Tachyon

实现Tachyon上的容错，需要为Tachyon master、worker和client添加额外的配置。在`conf/tachyon-env.sh`中，以下java选项需要设置：

<table class="table">
<tr><th>属性名</th><th>属性值</th><th>含义</th></tr>
{% for item in site.data.table.java-options-for-fault-tolerance %}
<tr>
  <td>{{item.PropertyName}}</td>
  <td>{{item.Value}}</td>
  <td>{{site.data.table.cn.java-options-for-fault-tolerance.[item.PropertyName]}}</td>
</tr>
{% endfor %}
</table>

设置这些选项，可以在`TACHYON_JAVA_OPTS`包含：

    -Dtachyon.zookeeper.enabled=true
    -Dtachyon.zookeeper.address=[zookeeper_hostname]:2181

如果集群有多个ZooKeeper节点，指定多个地址时用逗号分割：

    -Dtachyon.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

你也可以选择在`tachyon-site.properties`文件中配置以上的选项。更多配置参数选项请参考[配置设置](Configuration-Settings.html)。

### Master配置

除了以上配置，Tachyon master需要额外的配置，以下变量需在`conf/tachyon-env.sh`中正确设置：

    export TACHYON_MASTER_ADDRESS=[externally visible address of this machine]

同样，指定正确的日志文件夹需在`TACHYON_JAVA_OPTS`中设置`tachyon.master.journal.folder`，举例而言，如果使用HDFS来存放日志，可以添加：

    -Dtachyon.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/tachyon/journal

所有Tachyon master以这种方式配置后，都可以启动用于Tachyon的容错。其中一个成为leader，其余重播日志直到当前master失效。

### Worker配置

只要以上参数配置正确，worker就可以咨询ZooKeeper，找到当前应当连接的master。所以，worker无需设置`TACHYON_MASTER_ADDRESS`。

### Client配置

无需为容错模式配置更多的参数，只要以下两项：

    -Dtachyon.zookeeper.enabled=true
    -Dtachyon.zookeeper.address=[zookeeper_hostname]:2181

在client应用中正确设置，应用可以咨询ZooKeeper获取当前 leader master。
