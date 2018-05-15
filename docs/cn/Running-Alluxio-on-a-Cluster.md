---
layout: global
title: 在集群上独立运行Alluxio
nickname: 在集群上独立运行Alluxio
group: Deploying Alluxio
priority: 2
---

* 内容列表
{:toc}

## 使用单个Master运行Alluxio

在集群上部署Alluxio最简单的方法是使用单个master。但是，这个单个master在Alluxio集群中存在单点故障(SPOF)：如果该机器或进程不可用，整个集群将不可用。我们强烈建议在生产环境中使用具有[高可用性](#running-alluxio-with-high-availability)的模式来运行Alluxio masters。

### 下载Alluxio

为了在集群上部署Alluxio，首先要在每个节点下载Alluxio tar文件并解压：

为了在集群上部署Alluxio，首先 [下载](https://alluxio.org/download) Alluxio文件(.tar),然后在每个节点上解压它。

### 配置Alluxio

在`${ALLUXIO_HOME}/conf`目录下，从模板创建`conf/alluxio-site.properties`配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

更新`conf/alluxio-site.properties`中的`alluxio.master.hostname`为你将运行Alluxio Master的机器的主机名。添加所有worker节点的IP地址到`conf/workers`文件。
如果集群中存在多节点，你不可以使用本地文件系统作为Allxuio底层存储层。你需要在所有Alluxio服务端连接的节点启动共享存储，共享存储可以是
网络文件系统（NFS)，HDFS，S3等。例如。你可以参照[Configuring Alluxio with S3](Configuring-Alluxio-with-S3.html)按照说明启动S3作为Alluxio底层存储。

最后，同步所有信息到worker节点。你可以使用

```bash
$ ./bin/alluxio copyDir <dirname>
```

来同步文件和文件夹到所有的`alluxio/conf/workers`中指定的主机。如果你只在Alluxio master节点上下载并解压了Alluxio压缩包，你可以使用`copyDir`命令同步worker节点下的Alluxio文件夹，你同样可以
使用此命令同步`conf/alluxio-site.properties`中的变化到所有worker节点。

### 启动 Alluxio

现在，你可以启动 Alluxio:

```bash
$ cd alluxio
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh # use the right parameters here. e.g. all Mount
# Notice: the Mount and SudoMount parameters will format the existing RamFS.
```

为了确保Alluxio正在运行, 访问 `http://<alluxio_master_hostname>:19999`, 检查文件夹`alluxio/logs`下的日志, or 或者运行简单程序:

```bash
$ ./bin/alluxio runTests
```

**注意**: 如果你使用EC2, 确保master节点上的安全组设置允许来自alluxio web UI 端口的连接。

## 运行Alluxio实现高可用性

Alluxio的高可用性通过多master实现。同一时刻，系统中有多个master进程运行。其中一个被选举为leader，作为所有worker和
client的通信首选。其余master进入备用状态，和leader共享日志，以确保和leader维护着同样的文件系统元数据并在
leader失效时迅速接管leader的工作。

当前leader失效时，系统自动从可用的备用master中选举一个作为新的leader，Alluxio继续正常运行。但在切换到备用
master时，客户端会有短暂的延迟或瞬态错误。

### 前期准备

搭建一个高可用性的Alluxio集群需要两方面的准备：

* [ZooKeeper](http://zookeeper.apache.org/)
* 用于存放日志的可靠的共享底层文件系统。

Alluxio使用Zookeeper实现leader选举，可以保证在任何时间最多只有一个leader。

Alluxio使用共享底层文件系统存放日志。共享文件系统必须可以被所有master访问，可以选择
[HDFS](Configuring-Alluxio-with-HDFS.html)和
[GlusterFS](Configuring-Alluxio-with-GlusterFS.html)作为共享文件系统。leader master将日志写到共享文件
系统，其它(备用) master持续地重播日志条目与leader的最新状态保持一致。

#### ZooKeeper

Alluxio使用Zookeeper实现master的高可用性。Alluxio master使用Zookeeper选举leader。Alluxio client使用
Zookeeper查询当前leader的id和地址。

ZooKeeper必须单独安装
(见[ZooKeeper快速入门](http://zookeeper.apache.org/doc/r3.4.5/zookeeperStarted.html)).

部署Zookeeper之后，记下其地址和端口，下面配置Alluxio时会用到。

#### 存放日志的共享文件系统

Alluxio使用共享文件系统存放日志。所有master必须能够从共享文件系统进行读写。只有leader master可以在任何时
间写入日志，但所有master可以读取共享日志来重播Alluxio的系统状态。

共享文件系统必须单独安装（不能通过Alluxio），并且要在Alluxio启动之前处于运行状态。

举个例子，如果使用HDFS共享日志，记下NameNode的地址和端口，下面配置Alluxio时会用到。

### 配置Alluxio
Zookeeper和共享文件系统都正常运行时，需要在每个主机上配置好`alluxio-site.properties`。

#### 外部可见地址

下文中提到的“外部可见地址(externally visible address)”指的是机器上配置的接口地址，对Alluxio集群中其它节点可见。在EC2上，使用`ip-x-x-x-x`地址。而
且不能使用`localhost`或`127.0.0.1`，否则其它节点无法访问该结点。

#### 配置容错的Alluxio

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

设置这些选项，可以在`conf/alluxio-site.properties`包含：

    alluxio.zookeeper.enabled=true
    alluxio.zookeeper.address=[zookeeper_hostname]:2181

如果集群有多个ZooKeeper节点，指定多个地址时用逗号分割：

    alluxio.zookeeper.address=[zookeeper_hostname1]:2181,[zookeeper_hostname2]:2181,[zookeeper_hostname3]:2181

#### Master配置

除了以上配置，Alluxio master需要额外的配置。以下变量需在每一个Alluxio Master上的`conf/alluxio-site.properties`中正确设置：

   alluxio.master.hostname=[externally visible address of this machine]

同样，指定正确的日志文件夹需在`conf/alluxio-site.properties`中设置`alluxio.master.journal.folder`，举例而言，如果
使用HDFS来存放日志，可以添加：

    alluxio.master.journal.folder=hdfs://[namenodeserver]:[namenodeport]/path/to/alluxio/journal

所有Alluxio master以这种方式配置后，都可以启动用于Alluxio的高可用性。其中一个成为leader，其余重播日志直到当
前master失效。

#### Worker配置

只要以上参数配置正确，worker就可以咨询ZooKeeper，找到当前应当连接的master。所以，worker无需设置`alluxio.master.hostname`。

#### Client配置

无需为高可用性模式配置更多的参数，只要以下两项：

```properties
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=[zookeeper_hostname]:2181
```

在client应用中正确设置，应用可以咨询ZooKeeper获取当前 leader master。

#### HDFS API

如果使用HDFS API与高可用性模式的Alluxio通信，确保客户端的zookeeper配置正确。使用`alluxio://`模式但主机名和端口可以省略。在URL中的所有主机名都将被忽略，相应地，`alluxio.zookeeper.address`配置会被读取，从而寻找Alluxio leader master。

```
hadoop fs -ls alluxio:///directory
```

#### 自动故障处理

要测试自动故障处理，请ssh登录至当前的Alluxio master leader，并使用以下命令查找`AlluxioMaster`进程的进程ID：

```bash
$ jps | grep AlluxioMaster
```

然后使用以下命令杀死leader：

```bash
$ kill -9 <leader pid found via the above command>
```

然后可以使用以下命令查看leader：

```bash
$ ./bin/alluxio fs leader
```

命令的输出应该显示新的leader。您可能需要一段时间以等待新的leader当选。

访问Alluxio网页界面`http://{NEW_LEADER_MASTER_IP}:{NEW_LEADER_MASTER_WEB_PORT}`。 点击导航栏中的`Browse`，你会看到所有的文件都在那里。
