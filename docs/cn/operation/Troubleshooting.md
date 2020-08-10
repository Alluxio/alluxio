---
layout: global
title: 异常诊断与调试
group: Operations
priority: 8
---

* 内容列表
{:toc}

本页面主要是关于Alluxio使用过程中的一些指导和提示，方便用户能够更快的解决使用过程遇到的问题。

注意: 本页面的定位并不是指导解决Alluxio使用过程中遇到的所有问题。
用户可随时向[Alluxio邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)或其[镜像](http://alluxio-users.85194.x6.nabble.com/)提交问题。

## Alluxio日志地址

Alluxio运行过程中可产生master、worker和client日志，这些日志存储在`{ALLUXIO_HOME}/logs`文件夹中，日志名称分别为
`master.log`,`master.out`, `worker.log`, `worker.out` 和`user_${USER}.log`。其中`log`后缀的文件是log4j生成的，`out`后缀的文件是标准输出流和标准错误流重定向的文件。

master和worker日志对于理解Alluxio master节点和worker节点的运行过程是非常有帮助的，当Alluxio运行出现问题时，可以查阅日志发现问题产生原因。如果不清楚错误日志信息，可在[邮件列表](https://groups.google.com/forum/#!forum/alluxio-users)查找，错误日志信息有可能之前已经讨论过。

## Alluxio远程调试

Alluxio一般不在开发机上运行,这使得Alluxio的调试变得困难,我们通常会用"增加日志-编译-部署运行-查看日志"的方法来定位问题,而这种定位问题的效率比较低而且需要修改代码从新部署,这在有些时候是不允许的。

使用java远程调试技术可以简单、不修改源码的方式，进行源码级调试。你需要增加jvm 远程调试参数，启动调试服务。增加远程调试参数的方法有很多，比较方便的一种方法是，你可以在需要调试的节点上，在命令行中或`alluxio-env.sh`中配置环境变量，增加如下配置属性。

```console
$ export ALLUXIO_WORKER_JAVA_OPTS="$ALLUXIO_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6606"
$ export ALLUXIO_MASTER_JAVA_OPTS="$ALLUXIO_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6607"
$ export ALLUXIO_USER_DEBUG_JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6609"
```

特别的，如果你想调试shell命令，可以通过加上`-debug`标志来加上jvm调试参数`ALLUXIO_USER_DEBUG_JAVA_OPTS`来开启调试服务。例如`alluxio fs -debug ls /`。


`suspend = y/n` 会决定JVM进程是否等待直至调试器连接。如果你希望在命令行中进行调试，设置`suspend = y`。否则，设置 `suspend = n` ，这样就可以避免不必要的等待时间。

这样启动该节点上的master或者worker后，使用eclipse或intellij IDE等java开发环境，新建java远程调试配置，设置调试主机名和端口号，然后启动调试连接。如果你设置了断点并且到达了断点处，开发环境会进入调试模式，可以读写当前现场的变量、调用栈、线程列表、表达式评估，也可以执行单步进入、单步跳过、恢复执行、挂起等调试控制。掌握这个技术使得日后的定位问题事半功倍，也会对调试过的代码上下文印象深刻。

## Alluxio部署常见问题

### 问题: 在本地机器上初次安装使用Alluxio失败，应该怎么办？

解决办法: 首先检查目录`{ALLUXIO_HOME}/logs`下是否存在master和worker日志，然后按照日志提示的错误信息进行操作。否则，再次检查是否遗漏了[本地运行Alluxio]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }})里的配置步骤

典型问题:

- `ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS`配置不正确
- 如果 `ssh localhost` 失败, 请确认`~/.ssh/authorized_keys`文件中包含主机的ssh公钥

### 问题: 打算在Spark/HDFS集群中部署Alluxio，有什么建议？

解决办法: 按照[集群环境运行Alluxio]({{ '/cn/deploy/Running-Alluxio-on-a-Cluster.html' | relativize_url }}),
 [Alluxio配置HDFS]({{ '/cn/ufs/HDFS.html' | relativize_url }})提示操作。

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

### 问题：出现“java.lang.RuntimeException: java.lang.ClassNotFoundException: Class alluxio.hadoop.FileSystem not found”这种错误信息是什么原因？

解决办法：当你的应用（例如MapReduce、Spark）尝试以HDFS兼容文件系统接口访问Alluxio，并且`alluxio://`模式也已配置正确，但应用的classpath未包含Alluxio客户端jar包时会产生该异常。用户通常需要通过设置环境变量或者属性的方式将Alluxio客户端jar包添加到所有节点上的应用的classpath中，这取决于具体的计算框架。以下是一些示例：

- 对于MapReduce应用，可以将客户端jar包添加到`$HADOOP_CLASSPATH`：

```console
$ export HADOOP_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${HADOOP_CLASSPATH}
```

- 对于Spark应用，可以将客户端jar包添加到`$SPARK_CLASSPATH`：

```console
$ export SPARK_CLASSPATH={{site.ALLUXIO_CLIENT_JAR_PATH}}:${SPARK_CLASSPATH}
```

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

## Alluxio性能常见问题

### 问题: 在Alluxio/Spark上进行测试（对大小为GBs的文件运行单词统计），相对于HDFS/Spark，性能并无明显差异。为什么?

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
