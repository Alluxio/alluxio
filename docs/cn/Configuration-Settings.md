---
layout: global
title: 配置项设置
group: Features
priority: 1
---

* 内容列表
{:toc}

本页面介绍Alluxio的配置项，并提供了在不同环境下的推荐配置。

## Alluxio配置

Alluxio在运行时期会加载三种类型的配置源：

1. [应用配置](#application-settings)，这类配置是跟具体应用相关的，并且在每次运行一个应用（例如一个Spark作业）时都需要对其进行配置。
2. [环境变量](#environment-variables)，这是用于设置基本属性从而管理Alluxio服务器以及运行Alluxio shell命令最简单快速的方式。注意，通过环境变量设置的配置项可能不会被应用加载。
3. [属性文件](#property-files)，这是自定义[Alluxio支持的配置属性](#appendix)最一般的方法，通过这些属性文件设置的配置项可以被Alluxio服务器以及应用加载。

加载配置属性值的优先级从高到低依次为：应用配置（如果有），环境变量，属性文件以及默认值。

### 应用配置 {#application-settings}

Alluxio shell用户可以通过在命令行中添加`-Dkey=property`来指定某一个Alluxio配置项，例如

{% include Configuration-Settings/specify-conf.md %}

Spark用户可以在`conf/spark-env.sh`中将`"-Dkey=property"`添加到`${SPARK_DAEMON_JAVA_OPTS}`中，或者添加到`spark.executor.extraJavaOptions`（对于Spark executors）以及`spark.driver.extraJavaOptions`（对于Spark drivers）。

Hadoop MapReduce用户可以在`hadoop jar`命令中添加`-Dkey=property`将配置项传递给Alluxio：

{% include Configuration-Settings/hadoop-specify-conf.md %}

注意，这类配置是跟具体应用相关的，并且在每次运行一个应用或者命令（例如一个Spark作业）时都需要对其进行配置。

### 环境变量 {#environment-variables}

有许多常用的Alluxio配置项可以通过以下的环境变量进行配置：

<table class="table table-striped">
<tr><th>环境变量</th><th>意义</th></tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code></td>
  <td>Alluxio master的主机名，默认为localhost</td>
</tr>
<tr>
  <td><del><code class="highlighter-rouge">ALLUXIO_MASTER_ADDRESS</code></del></td>
  <td>从1.1版本开始被<code class="highlighter-rouge">ALLUXIO_MASTER_HOSTNAME</code>替代，并将在2.0版本中移除</td>
</tr>
<tr>
  <td><code class="highlighter-rouge">ALLUXIO_UNDERFS_ADDRESS</code></td>
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
</table>

例如，如果你希望将Alluxio master运行在`localhost`上，其底层存储系统HDFS的namenode也运行在`localhost`上，并且在7001端口启用Java远程调试，可以使用：

{% include Configuration-Settings/more-conf.md %}

用户可以通过shell命令或者`conf/alluxio-env.sh`设置这些环境变量。如果该文件不存在，可以通过运行以下命令令Alluxio自动生成`conf/alluxio-env.sh`文件：

{% include Common-Commands/bootstrapConf.md %}

除此之外，还可以运行以下命令从我们提供的一个模板文件中生成它：

{% include Common-Commands/copy-alluxio-env.md %}

注意，`conf/alluxio-env.sh`是在你[启动Alluxio系统](Running-Alluxio-Locally.html)或者[运行Alluxio命令](Command-Line-Interface.html)时被加载，而不是被应用加载。

### 属性文件 {#property-files}

除了以上这些提供基本设置的环境变量之外，Alluxio还为用户提供了一种更一般的方式，即通过属性文件来自定义所有支持的配置项。对于每个Alluxio部署站点，Alluxio服务器以及应用客户端都可以通过`alluxio-site.properties`文件覆盖默认属性值。在启动时，Alluxio会检查是否存在属性配置文件，如果存在，便会加载这些文件，并覆盖默认的属性值。启动程序将依次在`${HOME}/.alluxio/`，`/etc/alluxio/`（可以通过更改`alluxio.site.conf.dir`的默认值进行自定义）以及运行Alluxio的Java虚拟机的classpath中搜索该属性文件。

举个例子，用户可以复制`${ALLUXIO_HOME}/conf`目录下的属性模板保存到`${HOME}/.alluxio/`目录下，并且根据用户需求来调整配置属性的值。

{% include Common-Commands/copy-alluxio-site-properties.md %}

注意，一旦设置了，这些属性文件的配置项将在Alluxio服务器以及使用Alluxio客户端的作业中共享。

## 附录 {#appendix}

所有Alluxio配置属性都属于以下六类之一：
[共有配置项](#common-configuration)（由Master和Worker共享），
[Master配置项](#master-configuration)，[Worker配置项](#worker-configuration)，
[用户配置项](#user-configuration)，[集群管理配置项](#cluster-management)（用于在诸如Mesos和YARN的集群管理器上运行Alluxio）
以及[安全性配置项](#security-configuration)（由Master，Worker和用户共享）。

### 共有配置项 {#common-configuration}

共有配置项包含了不同组件共享的常量。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.common-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Master配置项 {#master-configuration}

Master配置项指定master节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.master-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### Worker配置项 {#worker-configuration}

Worker配置项指定worker节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.worker-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


### 用户配置项 {#user-configuration}

用户配置项指定了文件系统访问的相关信息。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.user-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### 集群管理配置项 {#cluster-management}

如果使用诸如Mesos和YARN的集群管理器运行Alluxio，还有额外的配置项。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.cluster-management[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### 安全性配置项 {#security-configuration}

安全性配置项指定了安全性相关的信息，如安全认证和文件权限。
安全认证相关的配置同时适用于master、worker和用户。
文件权限相关的配置只对master起作用。
更多安全性相关的信息详见[安全性](Security.html)页面。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.security-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.security-configuration[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

### 配置多宿主网络 {#configure-multihomed-networks}

Alluxio提供了一种使用多宿主网络的方式。如果你有多个NIC，并且想让Alluxio master监听所有的NIC，那么你可以将`alluxio.master.bind.host`设置为`0.0.0.0`，这样Alluxio client就可以通过任何一个NIC访问到master。其他以`bind.host`结尾的配置项也是类似的。
