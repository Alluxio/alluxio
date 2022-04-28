---
layout: global
title: 异常诊断与调试
group: Operations
priority: 8
---

* 内容列表
{:toc}

本页面主要是关于Alluxio使用过程中的一些指导和提示，方便用户能够更快的解决使用过程遇到的问题。

> 注意: 本页面的定位并不是指导解决Alluxio使用过程中遇到的所有问题。
用户可随时向[Alluxio邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)或其[镜像](http://alluxio-users.85194.x6.nabble.com/)提交问题。

## Alluxio日志地址

Alluxio运行过程中可产生master、worker和client日志，这些日志存储在`{ALLUXIO_HOME}/logs`文件夹中，日志名称分别为
`master.log`,`master.out`, `worker.log`, `worker.out` `job_master.log`, `job_master.out`,
`job_worker.log`, `job_worker.out` and `user/user_${USER}.log`。其中`log`后缀的文件是log4j生成的，`out`后缀的文件是标准输出流和标准错误流重定向的文件。

master和worker日志对于理解Alluxio master节点和worker节点的运行过程是非常有帮助的，当Alluxio运行出现问题时，可在[Github issue](https://github.com/Alluxio/alluxio/issues查找，错误日志信息有可能之前已经讨论过。
您也可以加入我们的 [Slack 频道](https://slackin.alluxio.io/) 并在那里寻求帮助。
您可以在 [此处]({{ '/en/operation/Basic-Logging.html#server-logs' | relativize_url }}) 找到有关 Alluxio 日志的更多详细信息。

当 Alluxio 运行在客户端侧无法连接的服务器上时，客户端侧的日志会很有用。 Alluxio客户端通过log4j生成日志消息，因此日志的位置由应用程序使用的客户端侧的 log4j 配置确定。
您可以在 [此处]({{ '/en/operation/Basic-Logging.html#application-logs' | relativize_url }}) 找到有关客户端日志的更多详细信息。

`${ALLUXIO_HOME}/logs/user/` 是 Alluxio shell 的日志。 每个用户都有单独的日志文件。

有关日志记录的更多信息，请查看
[本页]({{ '/en/operation/Basic-Logging.html' | relativize_url }})。

## Alluxio远程调试

Alluxio一般不在开发机上运行,这使得Alluxio的调试变得困难,我们通常会用"增加日志-编译-部署运行-查看日志"的方法来定位问题,而这种定位问题的效率比较低而且需要修改代码从新部署,这在有些时候是不允许的。

使用java远程调试技术可以简单、不修改源码的方式，进行源码级调试。你需要增加jvm 远程调试参数，启动调试服务。增加远程调试参数的方法有很多，比较方便的一种方法是，你可以在需要调试的节点上，在命令行中或`conf/alluxio-env.sh`中配置环境变量，增加如下配置属性。

```shell
# Java 8
export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60001"
export ALLUXIO_WORKER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60002"
# Java 11

export ALLUXIO_MASTER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60001"
export ALLUXIO_WORKER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60002"
```

### 调试 Shell 命令

特别的，如果你想调试shell命令(例如`alluxio fs -debug ls /`)，可以通过在 `conf/alluxio-env.sh` 中加上jvm调试参数`ALLUXIO_USER_DEBUG_JAVA_OPTS`来开启调试服务。
例如:

```shell
# Java 8
export ALLUXIO_USER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=60000"
# Java 11
export ALLUXIO_USER_ATTACH_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:60000"
```

`suspend = y/n` 会决定JVM进程是否等待直至调试器连接。如果你希望在命令行中进行调试，设置`suspend = y`。否则，设置 `suspend = n` ，这样就可以避免不必要的等待时间。

设置此参数后，可以添加`-debug`标志来启动调试服务器，例如`bin/alluxio fs -debug ls /`。

完成此设置后，了解 attach [attach](#to-attach)。

这样启动该节点上的master或者worker后，使用eclipse或intellij IDE等java开发环境，新建java远程调试配置，设置调试主机名和端口号，然后启动调试连接。如果你设置了断点并且到达了断点处，开发环境会进入调试模式，可以读写当前现场的变量、调用栈、线程列表、表达式评估，也可以执行单步进入、单步跳过、恢复执行、挂起等调试控制。掌握这个技术使得日后的定位问题事半功倍，也会对调试过的代码上下文印象深刻。

### To attach

参考 [关于如何在 IntelliJ 中 attaching 和 debug Java 进程的教程](https://www.jetbrains.com/help/idea/attaching-to-local-process.html)。

启动需要调试的进程或shell命令，然后创建一个新的java远程配置， 设置调试服务器的主机和端口，并启动调试会话.
如果你设置了可达的断点, IDE将进入调试模式. 您可以检查当前上下文中的变量、堆栈、线程列表和表达式

## Alluxio collectInfo 命令

Alluxio 的 `collectInfo` 命令，用于收集 Alluxio 集群信息以对 Alluxio 进行故障排除。`collectInfo` 将运行一系列子命令，每个子命令负责收集一类系统信息，详情可见下文
命令执行完毕后，所有收集到的信息将被打包到一个 tarball 中，其中包含大量有关你 Alluxio 集群的信息。 tarball 大小主要取决于你的集群大小以及你有多少子命令被执行。
例如，如果您有大量日志，“collectLog” 操作可能会很昂贵。通常其它命令不会生成大于 1MB 的文件。 tarball 中的信息将有助于你进行故障排除。
或者，您也可以与您信任的人共享 tarball，以帮助您排除 Alluxio 集群的故障。

`collectInfo` 命令将通过 SSH 连接到每个节点并执行一组子命令。
在执行结束时，收集的信息将被写入文件并打包。
每个单独的 tarball 将被收集到发布节点。
然后所有的 tarball 将被捆绑到最终的 tarball 中，其中包含有关 Alluxio 集群的所有信息。

> 注意：如果您的配置中包含了如 AWS 密钥等信息，请小心！
在与他人共享之前，您应该始终检查 tarball 中的内容并从 tarball 中删除敏感信息！

### 收集Alluxio集群信息
`collectAlluxioInfo` 将运行一组 Alluxio 命令来收集有关 Alluxio 集群的信息，如 `bin/alluxio fsadmin report` 等。
当 Alluxio 集群未运行时，该命令将无法收集到一些信息。
这个子命令将运行 `alluxio getConf` 以收集本地配置信息以及 `alluxio getConf --master --source` 命令来收集从 Master 接收到的配置信息。
它们都会隐藏证书相关字段的配置。不同的是，如果 Alluxio 集群没有启动，后一个命令会失败。

### 收集Alluxio配置文件
`collectConfig` 将收集 `${alluxio.work.dir}/conf` 目录下的所有配置文件。
从 Alluxio 2.4 开始 `collectAlluxioInfo` 将会运行 `alluxio getConf` 命令它打印所有配置属性，并隐藏证书相关字段,
而不是拷贝 `alluxio-site.properties` 文件， 因为很多用户会将UFS的明文证书信息放在此文件中。

[getConf 命令]({{ '/en/operation/User-CLI.html#getconf' | relativize_url }}) 将收集所有当前节点配置。

因此，如果想要收集压缩包中的 Alluxio 配置信息，请确保运行了 `collectAlluxioInfo` 子命令。

> 警告：如果你把证书明文放在除了alluxio-site.properties 之外的配置文件中（例如`alluxio-env.sh`），
除非您在 tarball 中手动清除了敏感信息，否则请勿与任何人共享收集的 tarball！

### 收集 Alluxio 日志
`collectLog` 将收集 `${alluxio.work.dir}/logs` 下的所有日志。

> 注意：在执行此命令前请粗略评估命令将收集的日志量！

### 收集 Alluxio 指标
`collectMetrics` 将收集在 `http://${alluxio.master.hostname}:${alluxio.master.web.port}/metrics/json/` 提供的 Alluxio 指标。
此命令将会执行多次以记录以检查程序执行进度。

### 收集JVM信息
`collectJvmInfo` 将收集每个节点上现有的 JVM 信息。 这是通过在每个找到的 JVM 进程上执行 `jps` 命令和 `jstack` 命令实现的。
此命令将执行多次以确认 JVM 是否正在运行。

###收集系统信息
`collectEnv` 将运行系列 bash 命令来收集有关 Alluxio 所在节点的信息。
此命令会运行系统命令，如 `env`、`hostname`、`top`、`ps` 等。

> 警告：如果您将证书字段存储诸如 AWS_ACCESS_KEY 等环境变量或进程启动参数中如`-Daws.access.key=XXX`，
不要与任何人分享你收集的压缩包，除非你在压缩包中手动清除了敏感信息！

### 收集上述所有信息
`all` 将运行上述的所有子命令。

### 命令选项

`collectInfo` 命令具有以下选项。

```shell
$ bin/alluxio collectInfo 
    [--max-threads <threadNum>] 
    [--local] 
    [--help]
    [--additional-logs <filename-prefixes>] 
    [--exclude-logs <filename-prefixes>] 
    [--include-logs <filename-prefixes>] 
    [--start-time <datetime>] 
    [--end-time <datetime>]
    COMMAND <outputPath>
```

`<outputPath>` 是 tarball 的输出目录

选项：
1. `--max-threads threadNum` 选项用于配置同时收集信息和传输 tarball 的线程数。
当集群有大量节点或大量日志文件时，用于传输 tarball 的网络流量可能会很大。 可以通过该参数来限制该命令的资源消耗。

1. `--local` 让 `collectInfo` 命令仅在当前节点上运行。 这意味着该命令将仅收集有关当前节点的信息。
如果您的集群没有配置节点间的 SSH 免密，您需要在集群中的每个节点上通过指定 `--local` 选项运行命令，并手动收集所有 tarball。
如果您的集群有配置节点间的 SSH 免密，您可以在不指定 --local 来运行命令，这将会把任务分发到每个节点并为您收集 tarball。

1. `--help` 打印帮助信息并退出。

1. `--additional-logs <filename-prefixes>` 指定要收集的额外日志文件名前缀。 默认情况下，`collectInfo` 命令只会收集 Alluxio 特定的日志文件。
收集的日志文件包括：
```
logs/master.log*, 
logs/master.out*, 
logs/job_master.log*, 
logs/job_master.out*, 
logs/master_audit.log*, 
logs/worker.log*, 
logs/worker.out*, 
logs/job_worker.log*, 
logs/job_worker.out*, 
logs/proxy.log*, 
logs/proxy.out*, 
logs/task.log*, 
logs/task.out*, 
logs/user/*
```
注意，`--additional-logs` 优先级将低于 `--exclude-logs` 
多个 `<filename-prefixes>` 以逗号分隔。

1. `--exclude-logs <filename-prefixes>` 指定要从默认收集列表中排除的文件名前缀。

1. `--include-logs <filename-prefixes>` 指定仅收集以此前缀开头的日志文件。 该选项不可与 `--additional-logs` 或 `--exclude-logs` 同时使用。

1. `--end-time <datetime>` 指定一个时间，此时间之后的日志将不会被收集。
   日志文件前几行将被读取，以推断日志的生成时间
   `<datetime>` 是一个时间格式的字符，例如 `2020-06-27T11:58:53`。
   支持的格式包括：
```
“2020-01-03 12:10:11,874”
“2020-01-03 12:10:11”
“2020-01-03 12:10”
“20/01/03 12:10:11”
“20/01/03 12:10”
2020-01-03T12:10:11.874+0800
2020-01-03T12:10:11
2020-01-03T12:10
```

1. `--start-time <datetime>`指定一个时间，此时间之前的日志将不会被收集。

## 资源泄漏检测

如果你正在操作你的 Alluxio 集群，你可能会注意到一个
日志中的消息，例如：

```
LEAK: <>.close() was not called before resource is garbage-collected. See https://docs.alluxio.io/os/user/stable/en/operation/Troubleshooting.html#resource-leak-detection for more information about this message.
```

Alluxio 有一个内置的探测机制来识别潜在的资源泄漏问题。此消息意味着 Alluxio 代码中存在 BUG 导致资源泄漏。
如果在集群操作期间出现此日志， 请 [创建一个 GitHub Issue](https://github.com/Alluxio/alluxio/issues/new/choose) 
报告并共享您的日志信息以及任何其它相关的信息。

默认情况下，Alluxio 在检测这些泄漏时，会对部分资源进行采样跟踪，并为每个被跟踪的资源记录该对象最近的访问信息。
采样和追踪将会消耗一定的资源。可以通过属性 `alluxio.leak.detector.level`来控制资源的消耗。
支持的选项包括

- `DISABLED`：不执行泄漏跟踪或记录，开销最低
- `SIMPLE`：只对泄漏进行采样和跟踪，不记录最近的访问，开销很小
- `ADVANCED`：采样和跟踪最近访问的资源，开销较高
- `PARANOID`：跟踪每个资源，开销最高

## Alluxio部署常见问题

### 问题: 在本地机器上初次安装使用Alluxio失败，应该怎么办？

解决办法: 首先检查目录`{ALLUXIO_HOME}/logs`下是否存在master和worker日志，然后按照日志提示的错误信息进行操作。
否则，再次检查是否遗漏了[本地运行Alluxio]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})里的配置步骤

典型问题:

- `ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS`配置不正确
- 如果 `ssh localhost` 失败, 请确认`~/.ssh/authorized_keys`文件中包含主机的ssh公钥

### 问题: 打算在Spark/HDFS集群中部署Alluxio，有什么建议？

解决办法: 按照[集群环境运行Alluxio]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }}),
[Alluxio配置HDFS]({{ '/cn/ufs/HDFS.html' | relativize_url }})。
和 [Apache Spark 使用 Alluxio]({{ '/cn/compute/Spark.html' | relativize_url }}) 提示操作.


提示:

- 通常情况下, 当Alluxio workers和计算框架的节点部署在一起的时候，性能可达到最优
- 如果你正在使用Mesos或者Yarn管理集群,也可以将Mesos和Yarn集成到Alluxio中，使用Mesos和Yarn可方便集群管理
- 如果底层存储是远程的，比如说S3或者远程HDFS,这种情况下，使用Alluxio会非常有帮助

## ALLuxio使用常见问题

### 问题：出现“No FileSystem for scheme: alluxio”这种错误信息是什么原因？

解决办法：当你的应用（例如MapReduce、Spark）尝试以HDFS兼容文件系统接口访问Alluxio，而又无法解析`alluxio://`模式时会产生该异常。要确保HDFS配置文件`core-site.xml`（默认在hadoop安装目录，如果为Spark自定义了该文件则在`spark/conf/`目录下）包含以下配置：

```xml
<configuration>
  <property>
    <name>fs.alluxio.impl</name>
    <value>alluxio.hadoop.FileSystem</value>
  </property>
</configuration>
```

有关详细的设置说明，请参见你的特定计算应用的文档页。

### 问题：出现“java.lang.RuntimeException: java.lang.ClassNotFoundException: Class alluxio.hadoop.FileSystem not found”这种错误信息是什么原因？

解决办法：当你的应用（例如MapReduce、Spark）尝试以HDFS兼容文件系统接口访问Alluxio，并且`alluxio://`模式也已配置正确，但应用的classpath未包含Alluxio客户端jar包时会产生该异常。用户通常需要通过设置环境变量或者属性的方式将Alluxio客户端jar包添加到所有节点上的应用的classpath中，这取决于具体的计算框架。以下是一些示例：

- 对于MapReduce应用，可以将客户端jar包添加到`$HADOOP_CLASSPATH`：

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```
See [MapReduce on Alluxio]({{ '/en/compute/Hadoop-MapReduce.html' | relativize_url }}) for more details.

- 对于Spark应用，可以将客户端jar包添加到`$SPARK_CLASSPATH`：

```console
$ export SPARK_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${SPARK_CLASSPATH}
```
See [Spark on Alluxio]({{ '/en/compute/Spark.html' | relativize_url }}) for more details.

除了上述方法，还可以将以下配置添加到`spark/conf/spark-defaults.conf`中：

```
spark.driver.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
spark.executor.extraClassPath {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

如果已经设置相关的classpath，但是异常仍然存在，用户可以这样检测路径是否有效：

```console
$ ls {{site.ALLUXIO_CLIENT_JAR_PATH}}
```

### 问题: 出现类似如下的错误信息: "Frame size (67108864) larger than max length (16777216)",这种类型错误信息出现的原因是什么?

解决办法: 多种可能的原因会导致这种错误信息的出现。

- 请仔细检查Alluxio的master节点的端口(port)是否正确，Alluxio的master节点默认的监听端口号为19998。
通常情况下master地址的端口号错误会导致这种错误提示的出现(例如端口号写成了19999,而19999是Alluxio的master节点的web用户界面的端口号)
- 请确保Alluxio的master节点和client节点的安全设置保持一致.
Alluxio通过配置`alluxio.security.authentication.type`来提供不同的[用户身份验证]({{ '/cn/operation/Security.html' | relativize_url }}#authentication)的方法。
如果客户端和服务器的这项配置属性不一致，这种错误将会发生。(例如，客户端的属性为默认值`NOSASL`,而服务器端设为`SIMPLE`)
有关如何设定Alluxio的集群和应用的问题，用户请参照[配置文档]({{ '/cn/operation/Configuration.html' | relativize_url }})
- Spark调用Alluxio-1.3.0文件时报错，如果是直接下载编译好的alluxio文件进行安装的，一般会出现该错误。
解决办法：需要Alluxio client需要在编译时指定Spark选项，具体参考[Spark应用配置文档]({{ '/cn/compute/Spark.html' | relativize_url }})；
编译好的依赖包也可以直接下载，下载地址：<a href="http://downloads.alluxio.io/downloads/files/1.3.0/alluxio-1.3.0-spark-client-jar-with-dependencies.jar"> 依赖包下载 </a>。

### 问题: 向Alluxio拷贝数据或者写数据时出现如下问题 "Failed to cache: Not enough space to store block on worker",为什么？

解决办法: 这种错误说明alluxio空间不足，无法完成用户写请求。

- 检查一下内存中是否有多余的文件并从内存中释放这些文件。查看[用户CLI]({{ '/cn/operation/User-CLI.html' | relativize_url }})获取更多信息。
- 通过改变`alluxio.worker.ramdisk.size`属性值增加worker节点可用内存的容量，查看[配置文档]({{ '/cn/operation/Configuration.html' | relativize_url }}#common-configuration) 获取更多信息。

### 问题： 当我正在写一个新的文件/目录，我的应用程序中出现日志错误。

解决办法： 当你看见类似"Failed to replace a bad datanode on the existing pipeline due to no more good datanodes being avilabe to try"。
这是因为Alluxio master还没有根据`alluxio.master.journal.folder`属性来更新HDFS目录下的日志文件。有多种原因可以导致这种类型的错误，其中典型的原因是：
一些用来管理日志文件的HDFS datanode处于高负载状态或者磁盘空间已经用完。当日志目录设置在HDFS中时，请确保HDFS部署处于连接状态并且能够让Alluxio正常存储日志文件。

###问：我在读取一些文件时看到一个错误 "Block ?????? is unavailable in both Alluxio and UFS"。我的文件在哪里？

A: 当向Alluxio写文件时，可以用几种写的类型之一来告诉Alluxio工作者数据应该如何存储。

`MUST_CACHE`: 数据将只存储在Alluxio中。

`CACHE_THROUGH`：数据将被缓存在Alluxio并写入UFS。

`THROUGH`: 数据将只被写入UFS

ASYNC_THROUGH"：数据将同步存储在Alluxio，然后异步写到UFS。

默认情况下，Alluxio客户端使用的写入类型是 "ASYNC_THROUGH"，因此写入Alluxio的新文件只存储在Alluxio
Worker存储，如果一个Worker崩溃了，可能会丢失。为了确保数据被持久化，要么使用`CACHE_THROUGH`或`THROUGH`写类型。
或者增加`alluxio.user.file.replication.durable`到一个可接受的冗余程度。

这个错误的另一个可能的原因是，该块存在于文件系统中，但没有工作者连接到主站。在这种情况下
这种情况下，一旦至少有一个包含该区块的工作器被连接，该错误就会消失。

###问：我正在运行一个Alluxio shell命令，它挂起了，没有任何输出。这是怎么回事？

答：大多数Alluxio shell命令需要连接到Alluxio主站才能执行。如果该命令不能连接到主站，它就会
如果命令不能连接到主站，它就会不断重试几次，表现为 "挂起 "很长时间。也有可能一些命令需要很长时间才能
执行，比如在一个慢速的UFS上持久化一个大文件。如果你想知道下面发生了什么，可以检查用户日志（存储为
默认存储为`${ALLUXIO_HOME}/logs/user_${USER_NAME}.log`）或主日志（默认存储为`${ALLUXIO_HOME}/logs/master.log`，位于主节点）。
节点）。

如果日志不足以揭示问题，你可以[启用更多的粗略日志]（{{ '/en/operation/Basic-Logging.html#enabling-advanced-logging' | relativize_url }}）。

###问：我收到未知的gRPC错误，如 "io.grpc.StatusRuntimeException: UNKNOWN"

答：一个可能的原因是RPC请求没有被服务器端识别。
这通常发生在你运行Alluxio客户端和Master/Worker的不同版本，其中的RPC不兼容。
请仔细检查，确保所有组件都运行相同的Alluxio版本。

如果你在上面没有找到答案，请在[这里](#posting-questions)之后发表一个问题。

## Alluxio性能常见问题

### 问题: 在Alluxio/Spark上进行测试（对大小为GBs的文件运行单词统计），相对于HDFS/Spark，性能并无明显差异。为什么?

解决办法: Alluxio通过使用分布式的内存存储（以及分层存储）和时间或空间的本地化来实现性能加速。如果数据集没有任何本地化, 性能加速效果并不明显。

## 环境

Alluxio在不同的生产环境下可配置不同的运行模式。
请确定当前Alluxio版本是最新的并且是支持的版本。

## 发布问题

我们强烈建议您搜索的问题是否已经被回答，问题是否已经被解决。Github issues 和 Slack 聊天记录都是非常好的来源。

当在 [Github issues](https://github.com/Alluxio/alluxio/issues)
或[Slack频道](https://alluxio.io/slack)，上提问是请附上完整的环境信息，包括
- Alluxio版本
- 操作系统版本
- Java版本
- UnderFileSystem 类型和版本
- Computing framework 的类型和版本
- 集群的信息，例如节点数量，每个节点的内存大小，数据中心内或跨数据中心的内存大小
- 相关的Alluxio配置，如`alluxio-site.properties`和`alluxio-env.sh`。
- 相关的Alluxio日志和计算/存储引擎的日志
- 如果你遇到了问题，请尝试通过清晰的步骤来重现它