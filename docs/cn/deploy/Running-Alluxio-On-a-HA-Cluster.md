---
layout: global
title: 在具有HA的群集上部署Alluxio
nickname: 具有HA的群集
group: Install Alluxio
priority: 3
---

* 目录
{:toc}

## 概述

Alluxio服务的高可用性（HA）是通过在系统多个不同节点上运行Alluxio master进程来实现的。这些master节点中的一个将被选为**leading master**做为首联节点服务所有workers节点。其他masters进程节点做为**standby masters**, standby masters通过跟踪共享日记来与leading master保持相同的文件系统状态。注意，standby masters不服务任何客户端或worker请; 但是，如果leading master出现故障，一个standby master会被自动选举成新的leading master来接管。一旦新的leading master开始服务，Alluxio客户端和workers将恢复照常运行。在故障转移到standby master期间，客户端可能会遇到短暂延迟或瞬态错误。

实现HA的主要挑战是在服务重新启动期间维持共享文件系统状态一致性和在故障转移后在所有masters中间保证对任何时间leading master选举结果的共识。在Alluxio 2.0中，有两种不同的方法可以实现这两个目标:

- [方法1](#option1-raft-based-embedded-journal): 使用基于RAFT的内部复制状态机来存储文件系统日志和leading master的选举。这种方法是在Alluxio 2.0中引入的，不需要依赖任何外部服务。
- [方法2](#option2-zookeeper-and-shared-journal-storage):
利用外部Zookeeper服务做leading master选举和利用共享存储（例如，root UFS）来共享日志。请参阅[日志管理文档]({{ '/en/operation/Journal.html' | relativize_url}})，以获取更多有关选择和配置Alluxio日记系统信息。

## 前提条件

* 要部署Alluxio群集，首先 下载 预编译的Alluxio二进制文件，解压缩tarball文件并将解压的目录复制到所有节点(包括运行master和worker的所有节点)。
* 激活不需要密码的从所有的master节点到所有的worker节点的SSH登录。 可以将主机的公共SSH密钥添加到`〜/.ssh/authorized_keys`中。 有关更多详细信息，请[参见本教程](http://www.linuxproblem.org/art_9.html)。
* 开放所有节点之间的TCP通信。 对于基本功能，确保所有节点上RPC端口都是打开的（default:19998）。

## 基本配置
### 选项1:基于Raft的嵌入式日志

HA集群的最小配置是将嵌入式日志地址提供给集群内的所有节点。在每个Alluxio节点上，依据模板创建`conf/alluxio-site.properties`配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

将以下属性添加到 `conf/alluxio-site.properties` 文件:

```properties
alluxio.master.hostname=<MASTER_HOSTNAME> # 仅在master节点上需要
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
alluxio.master.embedded.journal.addresses=<EMBEDDED_JOURNAL_ADDRESS>
```

说明:
- 第一个属性`alluxio.master.hostname=<MASTER_HOSTNAME>` 每个master节点上必须是其自身外部可访问主机名。master quorum的每个单独组成部分都需要具有自己的地址集。在worker节点上，此参数将被忽略。示例包括 `alluxio.master.hostname=1.2.3.4`， `alluxio.master.hostname=node1.a.com`。
- 第二个属性 `alluxio.master.mount.table.root.ufs=<STORAGE_URI>` 设置为挂载到Alluxio根目录的底层存储URI。 一定保证master节点和所有worker节点都可以访问此共享存储。 示例包括`alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`或`alluxio.master.mount.table.root.ufs=s3://bucket/dir/` 。
- 第三个属性 `alluxio.master.embedded.journal.addresses` 设置参加Alluxio leading master选举的master节点集。默认的嵌入式日志端口是 `19200`。例如: `alluxio.master.embedded.journal.addresses=master_hostname_1:19200，master_hostname_2:19200，master_hostname_3:19200`

嵌入式日记特性依赖于 [Copycat](https://github.com/atomix/copycat) 内置leader选举功能。内置leader选举功能不能与Zookeeper一起使用，因为系统不能出现多种leader选举机制选出不同leader的情况。启用嵌入式日记就启用了Alluxio的内置leader election机制。请参阅[嵌入式日志配置文档]({{ '/en/operation/Journal.html' | relativize_url}}＃embedded-journal-configuration)，以了解更多详细信息以及使用内部leader选举配置HA集群的替代方案。

### 选项2:Zookeeper和共享日志存储

设置Zookeeper HA集群的前提条件:
1. [ZooKeeper](http://zookeeper.apache.org/))集群。 Alluxio masters使用ZooKeeper进行leader选举，Alluxio客户端和workers使用ZooKeeper来查询当前谁是leading master。
1. 存放日志的共享存储系统(所有Alluxio masters均可访问)。leading master在此共享存储系统上写入日志，同时standby masters不断重播日志条目以保持与最新日志同步。建议将日志存储系统设置为:
  - 高可用性。所有master上元数据修改都需要写日志，因此日志存储系统的任何停机时间都将直接影响Alluxio master的可用性。
  - 设置共享存储系统为文件系统，而不要用对象存储。因为Alluxio master会写入存储系统的日志文件，和使用文件系统相应操作，例如重命名和flush。对象存储不是不支持这些操作就是即使支持执行速度很慢，因此，如果选择将日志存在对象存储中，Alluxio master的运行吞吐量将大幅降低。

必须设置的最小配置参数是:

```
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>
alluxio.master.journal.type=UFS
alluxio.master.journal.folder=<JOURNAL_URI>
```

说明:
- 属性 `alluxio.zookeeper.enabled=true masters`启用HA模式，并通知workers已启用HA模式。
- 属性 `alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>` ，`alluxio.zookeeper.enabled` 启用时设置ZooKeeper地址 。 HA masters将使用ZooKeeper进行leader选举。可以使用逗号分隔来指定多个ZooKeeper地址。实例包括 `alluxio.zookeeper.address =1.2.3.4:2181`，`alluxio.zookeeper.address=ZK1:2181，ZK2:2181，ZK3:2181`
- 属性 `alluxio.master.journal.type=UFS` 表示UFS被用来存放日志。注意，Zookeeper无法使用日志类型 EMBEDDED （使用masters中embedded日志）。
- 属性 `alluxio.master.journal.folder=<JOURNAL_URI>` 设置共享日志位置的URI，以供Alluxio leading master写入日志，以及做为standby masters重播日志条目依据。所有主节点都必须可以访问此共享存储系统。示例包括 `alluxio.master.journal.folder=hdfs://1.2.3.4:9000/alluxio/journal/`

确保所有master nodes和所有worker nodes都已正确配置了各自相应的 `conf/alluxio-site.properties` 配置文件。

一旦以上述方式配置了所有Alluxio masters和workers ，即可开始格式化和启动Alluxio。

#### Zookeeper的高级设置
对于具有较大规模名称空间的集群，leader上较大的CPU开销可能会导致Zookeeper客户端heartbeats延迟。因此，我们建议在名称空间大小超过几亿个文件的大型集群上将Zookeeper客户端session timeout设置为至少2分钟。
- `alluxio.zookeeper.session.timeout=120s`
  - Zookeeper服务器的最小/最大session timeout也必须配置为允许此timeout值。默认值要求timeout至少是的2倍`tickTime`(如服务器配置中设置)，最大是`tickTime`的20倍。也可以手动配置 `minSessionTimeout` 和 `maxSessionTimeout`。

Alluxio支持在Zookeeper leader选举中使用pluggable错误处理策略。
- `alluxio.zookeeper.leader.connection.error.policy` 指定如何处理连接错误。它可以是 `SESSION` 或 `STANDARD`。默认设置 为`SESSION` 。 

 `Session` 策略是利用Zookeeper sessions以确定leader状态是否健康。这意味着只要leader能够用同一session重新建立Zookeeper连接，暂停的连接不会直接触发目前leader退出。通过保持leader状态为系统提供了更好的稳定性。

`STANDARD` 策略把任何对zookeeper服务器的中断都视为错误。因此，即使其内部Zookeeper session与Zookeeper服务器之间没有任何问题，leader也将因错过心跳而退出。它为防止Zookeeper设置本身的错误和问题提供了更高的安全性。

## 启动具有HA的Alluxio集群

### 格式化Alluxio

在首次启动Alluxio之前，必须格式化日志。

> 格式化日志将删除Alluxio中的所有元数据。但是，将不会触及底层存储的数据。

在master节点上，使用以下命令格式化Alluxio:

```console
$ ./bin/alluxio format
```

### 启动Alluxio

如使用提供的脚本来启动Alluxio集群，在所有master节点上，在`conf/workers`文件中列出所有workers主机名。这将允许启动脚本在相应的节点上启动相应的进程。

在主节点上，运行以下命令启动Alluxio群集:

```console
$ ./bin/alluxio-start.sh all SudoMount
```

这将在`conf/masters`文件中指定的所有节点上启动 Alluxio master，并在`conf/workers`文件中指定的所有节点上启动所有workers。`SudoMount`参数使workers可以尝试使用`sudo`特权来挂载RamFS，如果尚未挂载。

### 验证Alluxio群集

要验证Alluxio是否正在运行，访问leading master的Web UI。要确定当前leading master，运行:

```console
$ ./bin/alluxio fs leader
```

然后，访问 `http://<LEADER_HOSTNAME>:19999` 以查看Alluxio leading master的状态页面。

Alluxio带有一个简单的程序可以在Alluxio中读写示例文件。 使用以下命令运行示例程序:

```console
$ ./bin/alluxio runTests
```

## 访问HA Alluxio集群

当应用程序在HA模式下与Alluxio交互时，客户端知道Alluxio HA集群，以便客户端知道如何返现Alluxio leading master。有三种方法可以在客户端上指定HA Alluxio服务地址:

### 在配置参数或Java Option中指定Alluxio服务

用户可以在环境变量或站点属性中预先配置Alluxio HA集群的服务地址，然后使用Alluxio URI连接服务， 如`alluxio:///path`其中连接HA集群所需详细信息已使用通过这些参数配置完成。例如，如果使用Hadoop，则可以在`core-site.xml`中配置属性，然后使用Hadoop CLI和Alluxio URI。

```console
$ hadoop fs -ls alluxio:///directory
```

根据实现HA的不同方法，需要设置不同的属性:

- 使用嵌入式日志方法连接到Alluxio HA集群时，设置属性`alluxio.master.rpc.addresses`来确定要查询的节点地址。例如添加如下设置到应用配置中:

```
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

或者通过Java Option传输给应用程序。比如对于Spark应用，将如下参数传给`spark.executor.extraJavaOptions`和`spark.driver.extraJavaOptions`:

```
-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```

- 使用Zookeeper连接到Alluxio HA集群时，需要以下属性设置才能连接到Zookeeper以获取leading Master信息。注意，当启用`alluxio.zookeeper.enabled`时必须指定ZooKeeper地址(`alluxio.zookeeper.address`) ，反之亦然。可以通过用逗号间隔来指定多个ZooKeeper地址

```
alluxio.zookeeper.enabled=true
alluxio.zookeeper.address=<ZOOKEEPER_ADDRESS>
```

### 使用URL Authority指定Alluxio服务{#ha-authority}

用户还可以通过在URI中完整描述HA集群信息的方式来连接到Alluxio HA集群。从HA Authority获取的配置优先于所有其他形式的配置，如站点属性或环境变量。

- 使用嵌入式日志时，使用 `alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/path`
- 使用Zookeeper做leader选举时，使用 `alluxio://zk@<ZOOKEEPER_ADDRESS>/path`。

对于许多应用程序(例如，Hadoop，HBase，Hive和Flink)，可以使用逗号作为URI中多个地址的分隔符，例如 `alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/path` 和 `alluxio://zk@zkHost1:2181,zkHost2:2181,zkHost3:2181/path`。

对于URL Authority内不接受逗号的其他一些应用程序(例如Spark)，需要使用分号作为多个地址的分隔符，例如 `alluxio://master_hostname_1:19998; master_hostname_2:19998; master_hostname_3:19998` 和 `alluxio://zk@zkHost1:2181; zkHost2:2181; zkHost3:2181/path`。

### 使用逻辑域名指定 Alluxio 服务

一些框架可能不接受上述两种方式来连接到高可用Alluxio HA集群，因此Alluxio也支持通过逻辑域名的来连接到Alluxio HA集群。为了使用逻辑域名，需要在环境变量或站点属性中设置以下的值。

#### 嵌入式日志逻辑域名

如果你使用的是嵌入式日志，你需要配置以下的值并通过`alluxio://ebj@[logical-name]`（例如 `alluxio://ebj@my-alluxio-cluster` ）来连接到高可用 alluxio 节点。

* alluxio.master.nameservices.[逻辑名称] 每个 alluxio master 节点的单独标识符

用逗号分割的 alluxio master 节点的 ID，用来确定集群中所有的 alluxio master 节点。例如，你之前使用 `my-alluxio-cluster`作为逻辑域名，并且想使用 `master1,master2,master3` 作为每个 alluxio master 的单独 ID，你可以这么设置：

```
alluxio.master.nameservices.my-alluxio-cluster=master1,master2,master3
```

* alluxio.master.rpc.address.[逻辑名称].[master 节点 ID] 每个 alluxio master 节点对应的地址

对于之前配置的每个 alluxio master 节点，设置每个 alluxio master 节点的完整地址，例如

```
alluxio.master.rpc.address.my-alluxio-cluster.master1=master1:19998
alluxio.master.rpc.address.my-alluxio-cluster.master2=master2:19998
alluxio.master.rpc.address.my-alluxio-cluster.master3=master3:19998
```

#### Zookeeper 逻辑域名

如果你使用 zookeeper 做 leader 选举时，你需要配置以下的值并通过`alluxio://zk@[logical-name]`（例如 `alluxio://zk@my-alluxio-cluster` ）来连接到高可用 alluxio 节点。

* alluxio.master.zookeeper.nameservices.[逻辑名称] 每个 Zookeeper 节点的单独标识符

用逗号分割的 Zookeeper 节点 ID，用来确定集群中所有的 Zookeeper 节点。例如，你之前使用 `my-alluxio-cluster`作为逻辑域名，并且想使用 `node1,node2,node3` 作为每个 Zookeeper 的单独 ID，你可以这么设置：

```
alluxio.master.zookeeper.nameservices.my-alluxio-cluster=node1,node2,node3
```

* alluxio.master.zookeeper.address.[逻辑域名].[Zookeeper 节点 ID] 每个 Zookeeper 节点对应的地址
  

对于之前配置的每个 Zookeeper 节点，设置每个 Zookeeper 节点的完整地址，例如

```
alluxio.master.zookeeper.address.my-alluxio-cluster.node1=host1:2181
alluxio.master.zookeeper.address.my-alluxio-cluster.node2=host2:2181
alluxio.master.zookeeper.address.my-alluxio-cluster.node3=host3:2181
```

## 常见操作

以下是在Alluxio集群上执行的常见操作。

### 停止Alluxio

要停止Alluxio服务，运行:

```console
$ ./bin/alluxio-stop.sh all
```

这将停止`conf/workers`和`conf/masters`中列出的所有节点上的所有进程。

可以使用以下命令仅停止master和worker:

```console
$ ./bin/alluxio-stop.sh masters # 停止所有conf/masters 的 masters  
$ ./bin/alluxio-stop.sh workers # 停止所有conf/workers 的 workers  
```

如果不想使用`ssh`登录所有节点来停止所有进程，可以在每个节点上运行命令以停止每个组件。 对于任何节点，可以使用以下命令停止master节点或worker节点:

```console
$ ./bin/alluxio-stop.sh master # 停止 local master
$ ./bin/alluxio-stop.sh worker # 停止 local worker
```

### 重新启动Alluxio

与启动Alluxio类似。 如果已经配置了conf/workers和conf/masters，可以使用以下命令启动集群:

```console
$ ./bin/alluxio-start.sh all
```

可以使用以下命令仅启动masters和workers:

```console
$ ./bin/alluxio-start.sh masters # starts all masters in conf/masters
$ ./bin/alluxio-start.sh workers # starts all workers in conf/workers
```

如果不想使用`ssh`登录所有节点来启动所有进程，可以在每个节点上运行命令以启动每个组件。 对于任何节点，可以使用以下命令启动master节点或worker节点:

```console
$ ./bin/alluxio-start.sh master # 开始 local master
$ ./bin/alluxio-start.sh worker # 开始 local worker
```

### 格式化日志

在任何master节点，运行以下命令格式化Alluxio日志:

```console
$ ./bin/alluxio format
```

> 格式化日志将删除Alluxio中的所有元数据。 但是，将不会触及底层存储的数据。

### 动态添加/删除worker

动态添加worker到Alluxio集群就像通过适当配置启动新Alluxio worker进程一样简单。 在大多数情况下，新worker配置应与所有其他worker配置相同。 在新worker上运行以下命令，以将其添加到集群。

```console
$ ./bin/alluxio-start.sh worker SudoMount # 开始 local worker
```

一旦启动，它将在Alluxio master上注册，并成为Alluxio集群的一部分。

删除worker就是停止worker进程这么简单。

```console
$ ./bin/alluxio-stop.sh worker # 停止 local worker
```

一旦worker被停止，master将在预定的超时值（通过master参数alluxio.master.worker.timeout配置）后将此worker标记为缺失。 主机视worker为“丢失”，并且不再将其包括在集群中。

### 添加/删除Masters
如要添加一个master节点，Alluxio集群必须已经在HA模式下运行。如果目前集群为单个master集群，则必须先将其配置为HA集群才能有多于一个master。

有关添加和删除masters的更多信息，请参见[日志管理文档]({{ '/en/operation/Journal.html' | relativize_url}})。

### 更新master侧配置

为了更新master配置，必须首先 [停止服务](#stop-alluxio)，更新master节点上的 `conf/alluxio-site.properties` 文件，然后 [重新启动服务](#restart-alluxio)。注意，这种方法会导致Alluxio服务停机。
作为替代方案，在HA模式下运行Alluxio的一个好处是可以使用滚动重启来最大程度地减少更新配置导致的停机时间:

1. 在不重启任何master情况下更新所有master节点master配置。
1. 重新启动leading master（可以通过运行 `bin/alluxio leader`确定当前leading master）。请注意，因为重启当前leading master, 新选出的leading master会保持服务连贯性。
1. 等待先前的leading master成功作为standby master完成启动。
1. 更新并重新启动所有剩余的standby masters
1. 验证配置更新

### 更新worker侧配置

如果只需要为worker节点更新某些本地配置（例如，更改分配给该worker的存储容量或更新存储路径），则无需停止并重新启动master节点。 可以只停止本地worker，更新此worker上的配置（例如`conf/alluxio-site.properties`）文件，然后重新启动此worker。
