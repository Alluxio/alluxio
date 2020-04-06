---
layout: global
title: 配置项设置
group: Operations
priority: 0
---

* Table of Contents
{:toc}

可以通过设置受支持的[系统配置参数](({{ '/en/reference/Properties-List.html' | relativize_url }}))来配置Alluxio。了解用户如何定制应用程序
(例如，Spark或MapReduce作业)与Alluxio交互，参见[如何配置引用应用程序](#configure-applications);要了解冲浪者管理员如何定制冲浪者服务，请参见[如何配置Alluxio集群](#configure-alluxio-cluster)。

# 配置应用 {#application-settings}

自定义应用程序作业如何与Alluxio服务交互是面向不同应用程序的。这里我们为一些常见的应用程序提供建议。

## Alluxio Shell 命令

在`fs` 命令和子命令之前(例如，`copyFromLocal`)，可以将JVM系统属性 `-Dproperty=value`放入到命令行中，以指定引用属性。例如，下面的Alluxio shell命令在将文件复制到Alluxio时将写入类型设置为`CACHE_THROUGH`:

```console
$ ./bin/alluxio fs -Dalluxio.user.file.writetype.default=CACHE_THROUGH \
copyFromLocal README.md /README.md
```

## Spark 作业
 
 Spark用户可以通过对Spark executor的`spark.executor.extraJavaOptions`和Spark drivers的`spark.driver.extraJavaOptions`
 添加`"-Dproperty=value"`向Spark job传递JVM环境参数。例如，当提交Spark jobs时设置向Alluxio写入方式为`CACHE_THROUGH`

```console
$ spark-submit \
--conf 'spark.driver.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
--conf 'spark.executor.extraJavaOptions=-Dalluxio.user.file.writetype.default=CACHE_THROUGH' \
...
```

在Spark shell中可以这样实现:

```scala
val conf = new SparkConf()
    .set("spark.driver.extraJavaOptions", "-Dalluxio.user.file.writetype.default=CACHE_THROUGH")
    .set("spark.executor.extraJavaOptions", "-Dalluxio.user.file.writetype.default=CACHE_THROUGH")
val sc = new SparkContext(conf)
```

## Hadoop MapReduce 作业

Hadoop MapReduce用户可以在`hadoop jar`或“`yarn jar`命令后添加`"-Dproperty=value"`。
属性将被传播到这个作业的所有任务中。例如,下面的
MapReduce任务中设置wordcount集写入Alluxio类型为`CACHE_THROUGH`：

```console
$ ./bin/hadoop jar libexec/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar wordcount \
-Dalluxio.user.file.writetype.default=CACHE_THROUGH \
-libjars {{site.ALLUXIO_CLIENT_JAR_PATH}} \
<INPUT FILES> <OUTPUT DIRECTORY>
```

# 配置Alluxio集群

## 使用 Site-Property 文件 (推荐) {#property-files}

Alluxio管理员可以创建和定制属性文件`alluxio-site.properties`来配置一个Alluxio集群。
如果该文件不存在，可以从模板文件`${ALLUXIO_HOME}/conf`:中创建:

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

确保在启动集群之前该文件上被分发到每个Alluxio节点(master和worker)的`${ALLUXIO_HOME}/conf` 下

## 使用集群默认

从v1.8开始，每个Alluxio客户端都可以使用从master节点获取的集群范围的配置值初始化其配置。
具体来说，当不同的客户端应用程序(如Alluxio Shell命令、Spark作业或MapReduce作业)连接到一个Alluxio服务时，
它们将使用master节点提供的默认值初始化自己的Alluxio配置属性，这些默认值是基于master节点的`${ALLUXIO_HOME}/conf/alluxio-site.properties`属性文件。因此，集群管理员可以放置客户端设置(例如，`alluxio.user.*`)或网络传输设置(如`alluxio.security.authentication.type`)在master节点的`${ALLUXIO_HOME}/conf/alluxio-site.properties`。
它将被分布并成为集群范围内的默认值，用于新的Aluxio客户端。

例如，一个常见的Alluxio属性`alluxio.user.file.writetype.default` 是设置写方式为默认的
`MUST_CACHE` ，只写到Alluxio空间。在一个Alluxio集群中
如果首选的是数据持久性的部署，所有的作业都需要写到UFS和Alluxio，那么就可以使用Alluxio v1.8或更高版本的admin命令来简单地添加`alluxio.user.file.writetype.default=CACHE_THROUGH` 到master端`${ALLUXIO_HOME}/conf/alluxio-site.properties`。重新启动集群后，所有新的作业都将自动将属性`alluxio.user.file.writetype.default`设置为`CACHE_THROUGH` 

客户端仍然可以忽略或覆盖集群范围内的默认值，通过指定属性`alluxio.user.conf.cluster.default.enabled=false`，
以更改加载集群范围内的默认值，或者遵循前面描述的方法[为应用程序配置文件]({{ '/cn/operation/Configuration.html' | relativize_url }}#configure-applications)覆盖相同的属性。

>注意到，在v1.8之前，`${ALLUXIO_HOME}/conf/alluxio-site.properties`属性的文件只被加载Alluxio服务器
>进程，并将被应用程序通过Alluxio客户端与Alluxio服务交互所忽略，
>除非`${ALLUXIO_HOME}/conf`在应用程序的类路径中。

## 使用环境变量 {#environment-variables}

Alluxio通过环境变量来支持一些常用的配置设置，包括:

<table class="table table-striped">
<tr><th>环境变量</th><th>意义</th></tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_CONF_DIR</code></td>
  <td>Alluxio配置目录的路径.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGS_DIR</code></td>
  <td>Alluxio logs目录的路径.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code></td>
  <td>Alluxio master的主机名，默认为localhost</td>
</tr>
<tr>
  <td><del><code class="highlighter-rouge">ALLUXIO_MASTER_ADDRESS</code></del></td>
  <td>从1.1版本开始被<code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code>替代，并将在2.0版本中移除</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS</code></td>
  <td>底层存储系统地址，默认为
<code class="highlighter-rouge">${ALLUXIO_HOME}/underFSStorage</code>，即本地文件系统</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_RAM_FOLDER</code></td>
  <td>Alluxio worker保存in-memory数据的目录，默认为<code class="highlighter-rouge">/mnt/ramdisk</code>.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_JAVA_OPTS</code></td>
  <td>Alluxio Master、Worker以及Shell中的Java虚拟机配置选项，注意，默认情况下<code class="highlighter-rouge">ALLUXIO_JAVA_OPTS</code>将被包含在
<code class="highlighter-rouge">ALLUXIO_MASTER_JAVA_OPTS</code>，
<code class="highlighter-rouge">ALLUXIO_WORKER_JAVA_OPTS</code>和
<code class="highlighter-rouge">ALLUXIO_USER_JAVA_OPTS</code>中。</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_JAVA_OPTS</code></td>
  <td>对Master配置的额外Java虚拟机配置选项</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_WORKER_JAVA_OPTS</code></td>
  <td>对Worker配置的额外Java虚拟机配置选项</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_USER_JAVA_OPTS</code></td>
  <td>对Alluxio Shell配置的额外Java虚拟机配置选项</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_CLASSPATH</code></td>
  <td>Alluxio进程的额外classpath入口. 默认为空</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_HOSTNAME</code></td>
  <td>log server的主机名.默认为空.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_PORT</code></td>
  <td>log server的端口名.默认为45600.</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_LOGSERVER_LOGS_DIR</code></td>
  <td>Alluxio log服务器存储从Alluxio服务器接收的log的本地目录路径.</td>
</tr>
</table>

例如，如果你希望将Alluxio master运行在`localhost`上，其底层存储系统HDFS的namenode也运行在`localhost`上，并且在7001端口启用Java远程调试，可以使用：

```console
$ export ALLUXIO_MASTER_HOSTNAME="localhost"
$ export ALLUXIO_MASTER_MOUNT_TABLE_ROOT_UFS="hdfs://localhost:9000"
$ export ALLUXIO_MASTER_JAVA_OPTS="$ALLUXIO_JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y, suspend=n,address=7001"
```

用户可以通过shell命令或者`conf/alluxio-env.sh`设置这些环境变量。如果该文件不存在，可以通过运行以下命令令Alluxio自动生成`conf/alluxio-env.sh`文件：

```console
$ cp conf/alluxio-env.sh.template conf/alluxio-env.sh
```

# 配置资源

Alluxio属性可以在多个资源中配置。在这种情况下，它的最终值由列表中最早的资源配置决定:

1. [JVM系统参数 (i.e., `-Dproperty=key`)](http://docs.oracle.com/javase/jndi/tutorial/beyond/env/source.html#SYS)
2. [环境变量](#use-environment-variables)
3. [参数配置文件](#use-site-property-files-recommended). 当Alluxio集群启动时, 每一个Alluxio服务端进程（包括master和worke） 在目录`${HOME}/.alluxio/`, `/etc/alluxio/` and `${ALLUXIO_HOME}/conf`下顺序读取 `alluxio-site.properties`
 , 当 `alluxio-site.properties` 文件被找到，将跳过剩余路径的查找.
4. [集群默认值](#use-cluster-default). Alluxio客户端可以根据master节点提供的集群范围的默认配置初始化其配置。

如果没有为属性找到上面用户指定的配置，那么会回到它的[默认参数值]({{ '/cn/reference/Properties-List.html' | relativize_url }})。

要检查特定配置属性的值及其值的来源，用户可以使用以下命令行:

```console
$ ./bin/alluxio getConf alluxio.worker.rpc.port
29998
$ ./bin/alluxio getConf --source alluxio.worker.rpc.port
DEFAULT
```

列出所有配置属性的来源:

```console
$ ./bin/alluxio getConf --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```

用户还可以指定`--master`选项来通过master节点列出所有的集群默认配置属性
。注意，使用`--master`选项 `getConf`将查询master，因此需要主节点运行;没有`--master` 选项，此命令只检查本地配置。

```console
$ ./bin/alluxio getConf --master --source
alluxio.conf.dir=/Users/bob/alluxio/conf (SYSTEM_PROPERTY)
alluxio.debug=false (DEFAULT)
...
```
