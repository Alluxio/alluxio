---
layout: global
title: 配置项设置
group: Features
priority: 1
---

* Table of Contents
{:toc}

Alluxio有两种类型的配置参数：

1. [配置属性](#configuration-properties)用来配置Alluxio系统运行时的设置;
2. [系统环境属性](#system-environment-properties)用来控制运行Alluxio的Java VM的选项以及一些基本设置。

# 配置属性 {#configuration-properties}

Alluxio启动时会加载默认（也可以指定）配置属性文件从而设置配置属性。

1. Alluxio的配置属性默认值在`alluxio-default.properties`文件中，该文件在Alluxio源代码或者二进制包里都能找到，不建议初学者直接修改该文件。

2. 每个部署点以及应用客户端都能够通过`alluxio-site.properties`文件将默认属性值覆盖掉，注意该文件必须在Alluxio Java VM的**classpath**中，最简单的方法是将该属性文件放在`$ALLUXIO_HOME/conf`目录中，并根据你的配置调节的需求对其进行编辑。

所有Alluxio配置属性都属于以下六类之一：
[共有配置项](#common-configuration)（由Master和Worker共享），
[Master配置项](#master-configuration)，[Worker配置项](#worker-configuration)，
[用户配置项](#user-configuration)，[集群管理配置项](#cluster-management)（用于在诸如Mesos和YARN的集群管理器上运行Alluxio）
以及[安全性配置项](#security-configuration)（由Master，Worker和用户共享）。

## 共有配置项 {#common-configuration}

共有配置项包含了不同组件共享的常量。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.common-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.common-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Master配置项 {#master-configuration}

Master配置项指定master节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.master-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.master-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## Worker配置项 {#worker-configuration}

Worker配置项指定worker节点的信息，例如地址和端口号。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.worker-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.worker-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>


## 用户配置项 {#user-configuration}

用户配置项指定了文件系统访问的相关信息。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.user-configuration %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.user-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 集群管理配置项 {#cluster-management}

如果使用诸如Mesos和YARN的集群管理器运行Alluxio，还有额外的配置项。

<table class="table table-striped">
<tr><th>属性名</th><th>默认值</th><th>意义</th></tr>
{% for item in site.data.table.cluster-management %}
  <tr>
    <td>{{ item.propertyName }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.cluster-management.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 安全性配置项 {#security-configuration}

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
    <td>{{ site.data.table.cn.security-configuration.[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

## 配置多宿主网络 {#configure-multihomed-networks}

Alluxio提供了一种使用多宿主网络的方式。如果你有多个NIC，并且想让Alluxio master监听所有的NIC，那么你可以将`alluxio.master.bind.host`设置为`0.0.0.0`，这样Alluxio client就可以通过任何一个NIC访问到master。其他以`bind.host`结尾的配置项也是类似的。

# 系统环境属性 {#system-environment-properties}

要运行Alluxio，还需要配置一些系统环境变量，默认情况下，这些变量在`conf/alluxio-env.sh`文件中被定义，如果该文件不存在，你可以从源代码文件夹中的template文件复制得到：

{% include Common-Commands/copy-alluxio-env.md %}

有许多频繁用到的Alluxio配置项可以通过环境变量设置，可以通过Shell设置或者在`conf/alluxio-env.sh`文件中修改其默认值。

* `$ALLUXIO_MASTER_ADDRESS`: Alluxio master地址，默认为localhost。
* `$ALLUXIO_UNDERFS_ADDRESS`: 底层文件系统地址，默认为`${ALLUXIO_HOME}/underFSStorage`，即本地文件系统。
* `$ALLUXIO_JAVA_OPTS`: 针对Master和Workers的Java VM选项。
* `$ALLUXIO_MASTER_JAVA_OPTS`: 针对Master配置的额外Java VM选项。
* `$ALLUXIO_WORKER_JAVA_OPTS`: 针对Worker配置的额外Java VM选项，注意，默认情况下，`ALLUXIO_JAVA_OPTS`被包含在`ALLUXIO_MASTER_JAVA_OPTS`和`ALLUXIO_WORKER_JAVA_OPTS`中。

例如，如果你需要将Alluxio与本地的HDFS相连接，并在7001端口启用Java远程调试，可以使用以下命令：

{% include Configuration-Settings/more-conf.md %}
