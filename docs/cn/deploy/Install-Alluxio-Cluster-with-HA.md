---
布局： 全局
title： 使用 HA 安装 Alluxio 群集
---
## 概览


具有高可用性（HA）的 Alluxio 群集是通过在系统的不同节点上运行多个 Alluxio 主
进程来实现。
其中一个主进程被选为**领导主进程**，作为主要联络点为所有工作站和客户端提供服务。
主要联络点。
其他主进程作为**备用主进程**，通过读取共享日志来保持与主导主进程相同的文件系统状态。
的文件系统状态。
备用主站不处理任何客户端或工作者的请求；但是，如果主导主站发生故障、
一个备用主站将自动被选为新的主导主站。
一旦新的主导主站开始提供服务，Alluxio 客户端和工作站将照常运行。
在故障切换到备用主站期间，客户端可能会出现短暂的延迟或瞬时错误。


实现高可用性的主要挑战是在服务重启时保持共享文件系统状态，以及在服务重启后保持共识。
状态，并在主站之间就故障后**主导主站**的身份保持一致。
故障切换后的**主导主**身份保持一致。
[基于船筏的日志]（#基于船筏的嵌入式日志）： 使用内部复制状态机
基于 Raft 协议](https://raft.github.io/) 的内部复制状态机来存储文件系统日志和运行领导者选举。
领导者选举。
这种方法在 Alluxio 2.0 中引入，无需依赖外部服务。




## 前提条件


* 要部署 Alluxio 集群，首先要 [下载](https://alluxio.io/download) 预编译的
  预编译的 Alluxio 二进制文件，解压压缩包并将解压后的目录复制到所有节点（包括运行主节点和工作节点的节点）。
  节点）。
* 启用从所有主节点到所有工作节点的无密码 SSH 登录。您可以将主机的 SSH 公钥
  可将主机的 SSH 公钥添加到 `~/.ssh/authorized_keys` 中。参见
  [本教程](http://www.linuxproblem.org/art_9.html) 了解更多详情。
* 允许跨所有节点的 TCP 通信。为实现基本功能，请确保所有节点上的 RPC 端口（默认 :19998）都已打开。
  在所有节点上打开。


### 基本设置


Alluxio 管理员可以创建并编辑属性文件 `conf/alluxio-site.properties` 来配置 Alluxio 主站或服务器。
配置 Alluxio 主站或工作站。
如果该文件不存在，可从 `${ALLUXIO_HOME}/conf` 下的模板文件中复制：


```shell
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```


确保在启动群集前将此文件分发到每个 Alluxio 主站和工作站上的 `${ALLUXIO_HOME}/conf
和 Worker 上的`${ALLUXIO_HOME}/conf
重启 Alluxio 进程是确保应用任何配置更新的最安全方法。


### 基于 Raft 的嵌入式日志


建立 HA 群集的最低配置是为群集内的所有节点提供嵌入式日志地址。
在每个 Alluxio 节点上，复制 `conf/alluxio-site.properties` 配置文件，并在文件中添加以下属性：

```properties
alluxio.master.hostname=<MASTER_HOSTNAME> # 仅主节点需要
alluxio.master.embedded.journal.addresses=<EMBEDDED_JOURNAL_ADDRESS> # 仅主节点需要
```

解释：
- 每个主节点都需要第一个属性 `alluxio.master.hostname=<MASTER_HOSTNAME> `，以作为自己的对外可见属性。
  是其外部可见的主机名。
  主节点法定人数的每个单独组件都需要使用该参数，以便拥有自己的地址集。
  在工作节点上，该参数将被忽略。
  例如，"alluxio.master.hostname=1.2.3.4"、"alluxio.master.hostname=node1.a.com"。
- 第二个属性 "alluxio.master.embedded.journal.address "设置了参与 Alluxio 内部领导者的主节点集。
  设置主节点集，以便参与 Alluxio 内部的领导者选举，并确定领导主节点。
  默认的嵌入式日志端口为 19200。
  例如：`alluxio.master.embedded.journal.address=master_hostname_1:19200,master_hostname_2:19200,master_hostname_3:19200`。


请注意，嵌入式日志功能依赖于 [Ratis](https://github.com/apache/ratis)，后者使用基于 Raft 协议的领导者选举。
它使用基于 Raft 协议的领导者选举，并有自己的日志条目存储格式。
启用嵌入式日志可启用 Alluxio 的内部领导者选举。
请参阅[嵌入式日志配置文档]（{{ '/en/operation/Journal.html | relativize_url }}#configuring-embedded-journal)
了解更多详情，以及使用内部领导者选举建立 HA 集群的其他方法。


### 使用 HA 启动 Alluxio 群集


### 格式化 Alluxio


首次启动 Alluxio 之前，必须格式化 Alluxio 主日志和工作存储。


> 格式化日志将删除 Alluxio 中的所有元数据。
> 格式化工作存储将删除配置的 Alluxio 存储中的所有数据。
> 但是，存储中的数据不会被触动。


在所有 Alluxio 主节点上，在 "conf/workers "文件中列出所有 Worker 主机名，并在 "conf/masters "文件中列出所有主节点。
这将允许 Alluxio 脚本在集群节点上运行操作。
在其中一个主节点上使用以下命令启动格式化 Alluxio 群集：


```shell
$ ./bin/alluxio init format
```


### 启动 Alluxio

这将在 `conf/masters` 中指定的所有节点上启动 Alluxio 主节点，并在 `conf/workers` 中指定的所有节点上启动工作者节点。
中指定的所有节点上启动 Worker。


### 验证 Alluxio 群集


要验证 Alluxio 是否正在运行，可以访问主导主节点的 Web UI。
要确定master,运行:

```shell
$ ./bin/alluxio info report
```

然后，访问 `http://<LEADER_HOSTNAME>:19999` 查看 Alluxio 主导主站的状态页面。


Alluxio 自带一个简单的程序，用于在 Alluxio 中写入和读取示例文件。运行示例程序：

```shell
$ ./bin/alluxio exec basicIOTest
```

## 使用 HA 访问 Alluxio 集群


当应用程序在 HA 模式下与 Alluxio 交互时，客户端必须了解
Alluxio HA 集群的连接信息，以便客户端知道如何发现 Alluxio 主导主站。
下文列出了在客户端指定 HA Alluxio 服务地址的三种方法。


### 在配置参数或 Java 选项中指定 Alluxio 服务


用户可在环境变量或站点属性中预先配置 Alluxio HA 群集的服务地址
或站点属性中预先配置服务地址，然后使用 Alluxio URI（如 `alluxio:///path`）连接到服务。
例如，有了 Hadoop 的 `core-site.xml` 中的 Alluxio 连接信息，Hadoop CLI 就可以 
连接到 Alluxio 集群。


```shell
$ hadoop fs -ls alluxio:///directory
```

根据实现 HA 的不同方法，需要不同的属性：


如果使用嵌入式日志，请设置 ``alluxio.master.rpc.addresses`.


```properties
alluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```


或在 Java 选项中指定属性。例如，对于 Spark 应用程序，将以下内容添加到 
spark.executor.extraJavaOptions`和`spark.driver.extraJavaOptions`：


```properties
-Dalluxio.master.rpc.addresses=master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998
```


### 使用 URL 权限指定 Alluxio 服务 {#ha-authority}


用户还可以在 URI 中完全指定 HA 群集信息，以连接到 Alluxio HA 群集。
来自 HA 授权的配置优先于所有其他形式的配置、
如站点属性或环境变量。


- 使用嵌入式日志时，请使用 `alluxio://master_hostname_1:19998、
master_hostname_2:19998,master_hostname_3:19998/path`


对于许多应用程序（如 Hadoop、Hive 和 Flink），您可以使用逗号作为
作为 URI 中多个地址的分隔符，如
alluxio://master_hostname_1:19998,master_hostname_2:19998,master_hostname_3:19998/path`。


对于某些其他应用程序（如 Spark），由于 URL 权限内不接受逗号，因此需要使用分号作为
需要使用分号作为多个地址的分隔符、
如`alluxio://master_hostname_1:19998;master_hostname_2:19998;master_hostname_3:19998`。


### 使用逻辑 URL 权限指定 Alluxio 服务 


某些框架可能不接受上述两种连接高可用 Alluxio HA 集群的方式、
因此，Alluxio 也支持通过逻辑名称连接到 Alluxio HA 集群。要使用逻辑
名称，需要在环境变量或站点属性中设置以下配置选项。


#### 使用嵌入式日志时使用逻辑名称
 
如果使用嵌入式日志，则需要配置以下配置选项并连接到高可用的 Alluxio 节点。
连接到高可用的 alluxio 节点，例如
`alluxio://ebj@my-alluxio-cluster`.


* 每个 alluxio 主节点的唯一标识符。


以逗号分隔的 alluxio 主节点 ID，用于确定群集中的所有 alluxio 主节点。
例如，如果您以前使用`my-alluxio-cluster`作为逻辑名称，并希望使用
master1,master2,master3 "作为每个 alluxio 主节点的独立 ID，则应这样配置：


```properties
alluxio.master.nameservices.my-alluxio-cluster=master1,master2,master3
```


* alluxio.master.rpc.address. 每个 alluxio 主节点的 [主节点 ID] RPC 地址


为先前配置的每个 alluxio 主节点设置每个 alluxio 主节点的完整地址，例如


```properties
alluxio.master.rpc.address.my-alluxio-cluster.master1=master1:19998
alluxio.master.rpc.address.my-alluxio-cluster.master2=master2:19998
alluxio.master.rpc.address.my-alluxio-cluster.master3=master3:19998
```


### 常用操作


以下是在 Alluxio 集群上执行的常见操作。


### 停止 Alluxio


要停止 Alluxio 服务，请运行


```shell
$ ./bin/alluxio process stop all
```


这将停止 `conf/workers` 和 `conf/masters` 中列出的所有节点上的所有进程。


使用以下命令可以只停止主进程和工作者进程：


```shell
$ ./bin/alluxio process stop masters # 停止 conf/masters 中的所有主节点
$ ./bin/alluxio process stop workers # 停止 conf/workers 中的所有工作者
```

如果不想使用 `ssh` 登录所有节点并停止所有进程，可以在每个节点上单独运行
命令来停止每个组件。
对于任何节点，都可以使用以下命令停止主进程或工作进程：


```shell
$ ./bin/alluxio process stop master # 停止本地主进程
$ ./bin/alluxio process stop worker # 停止本地 Worker
```


### 重启 Alluxio


启动 Alluxio 与此类似。如果 `conf/workers` 和 `conf/masters` 都已填充，则可以用以下命令启动集群
集群：


```shell
$ ./bin/alluxio process start all
```


您可以使用以下命令仅启动主进程和工作者进程：


```shell
$ ./bin/alluxio 进程启动主进程 # 启动 conf/masters 中的所有主进程
$ ./bin/alluxio 进程启动工作者 # 启动 conf/workers 中的所有工作者
```


如果不想使用 `ssh` 登录所有节点并启动所有进程，可以在每个节点上单独运行
命令来启动每个组件。对于任何节点，都可以通过以下命令启动主进程或工作者：

```shell
$ ./bin/alluxio process start master # 启动本地主进程
$ ./bin/alluxio process start worker # 启动本地 Worker
```


### 动态添加/移除 Worker

向 Alluxio 集群动态添加 Worker 就像启动一个新的 Alluxio Worker
进程。
在大多数情况下，新 Worker 的配置应与所有其他 Worker 的配置相同。
在新 Worker 上运行以下命令以添加

```shell
$ ./bin/alluxio process start worker # 启动本地 Worker
```

一旦启动 Worker，它就会向 Alluxio 主导主控程序注册，成为 Alluxio 集群的一部分。

删除 Worker 就像停止 Worker 进程一样简单。

```shell
$ ./bin/alluxio process stop worker # 停止本地 Worker
```

一旦停止 Worker，并在
超时（由主控参数 `alluxio.master.worker.timeout` 配置）后，主控会将 Worker 视为 "丢失"。
会将 Worker 视为 "丢失"，不再将其视为群集的一部分。

### 添加/删除 Master

要添加主站，Alluxio 群集必须在 HA 模式下运行。
如果将群集作为单个主控群集运行，则必须将其配置为 HA 群集，然后才能拥有多个主控。
配置为 HA 集群。

有关添加和删除主站的更多信息，请参阅 [日志管理文档]（{{ '/en/operation/Journal.html#adding-a-new-master' | relativize_url }}）。
有关添加和删除主站的更多信息。

### 更新主节点配置

要更新主节点配置，可以先[停止服务](#stop-alluxio)、
更新主节点上的 `conf/alluxio-site.properties` 文件、
然后 [重新启动服务](#restart-alluxio)。
请注意，这种方法会导致 Alluxio 服务停机。

另外，在 HA 模式下运行 Alluxio 的一个好处是使用滚动重启
以减少更新配置时的停机时间：

1. 在不重启任何主节点的情况下更新所有主节点上的主配置。
2. 2. 逐个重启备用主节点（群集[无法承受超过 `floor(n/2)` 同时重启]( {{ '/en/operation/Journal.html#embedded-journal-vs-ufs-journal' | relativize_url }})）。
3. 选举一个备用主站作为领导主站（教程 [此处]({{ '/en/operation/Journal.html#electing-a-specific-master-as-leader' | relativize_url }})）。
4. 重新启动现在是备用主站的旧主站。
5. 验证配置更新。

### 更新工作站配置

如果只需要更新 Worker 的某些本地配置（例如，更改分配给该 Worker 的存储容量的挂载或更新存储目录），则不需要更新 Worker 的配置。
分配给该 Worker 的存储容量或更新存储目录），则主节点无需停止并重新启动。
无需停止和重新启动。
只需停止所需的运行程序，更新配置
(例如，"conf/alluxio-site.properties"）文件，然后重新启动进程。

