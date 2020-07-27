---
layout: global
title: 在集群上独立运行Alluxio
nickname: 在集群上独立运行Alluxio
group: Install Alluxio
priority: 2
---

* 内容列表
{:toc}

## 使用单个Master运行Alluxio

在集群上部署Alluxio最简单的方法是使用单个master。
但是，这个单个master在Alluxio集群中存在单点故障(SPOF)：如果该机器或进程不可用，整个集群将不可用。
我们强烈建议在生产环境中使用具有[高可用性](#running-alluxio-with-high-availability)的模式来运行Alluxio masters。

## 先决条件

* 要部署Alluxio群集，首先[下载](https://www.alluxio.io/download/) 预编译的Alluxio二进制文件，使用以下命令解压缩tarball，并将解压的目录复制到所有节点（包括运行master和worker的所有节点）

```console
$ tar -xvzpf alluxio-{{site.ALLUXIO_VERSION_STRING}}-bin.tar.gz
```

*设置不需要密码的从master节点到worker节点的SSH登录。 
可以将主机的公共SSH密钥添加到`〜/.ssh/authorized_keys`中。
有关更多详细信息，请参见[本教程](http://www.linuxproblem.org/art_9.html)。

*开放所有节点之间的TCP通信。 对于基本功能，确保所有节点上RPC端口都是打开的（默认值：19998）。

*仅在期望Alluxio自动在worker节点上上安装RAMFS时才需要给将运行Allluxio的OS用户授予sudo特权。

## 基本配置

在master节点上，参照模板创建`conf/alluxio-site.properties`配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

在配置文件（`conf/alluxio-site.properties`）中按如下配置：

```
alluxio.master.hostname=<MASTER_HOSTNAME>
alluxio.master.mount.table.root.ufs=<STORAGE_URI>
```

- 第一个属性`alluxio.master.hostname`设置单个master节点的主机名。 示例包括`alluxio.master.hostname=1.2.3.4`或`alluxio.master.hostname=node1.a.com`。
- 第二个属性`alluxio.master.mount.table.root.ufs`设置为挂载到Alluxio根目录的底层存储URI。 一定保证master节点和所有worker节点都可以访问此共享存储。 示例包括`alluxio.master.mount.table.root.ufs=hdfs://1.2.3.4:9000/alluxio/root/`或`alluxio.master.mount.table.root.ufs=s3//bucket/dir/`。

接下来，将配置文件复制到所有其他Alluxio节点。 通过将所有work节点的IP地址或主机名添加到`conf/workers`文件中，操作员可以利用内置工具将配置文件复制到远程节点，如下所示。

```console
$ ./bin/alluxio copyDir conf/
```

此命令会将conf/目录复制到`conf/workers`文件中指定的所有worker节点。 
成功执行此命令后，所有Alluxio节点都将被正确配置。
这是启动Alluxio的最低配置，用户可以添加其他配置。


## 启动一个Alluxio集群

### 格式化Alluxio

在首次启动Alluxio之前，必须先格式化日志。

> 格式化日记将删除Alluxio中的所有元数据。 但是，格式化不会涉及底层存储的数据。

在master节点上，使用以下命令格式化Alluxio：

```console
$ ./bin/alluxio formatMaster
```

### 启动Alluxio

启动Alluxio集群，在master点上确保`conf/workers`文件中所有worker的主机名都是正确的。

在master点上，运行以下命令启动Alluxio集群：

```console
$ ./bin/alluxio-start.sh all SudoMount
```

这将在此节点上启动master，并在`conf/workers`文件中指定的所有节点上启动所有workers。 `SudoMount`参数使workers可以尝试使用`sudo`特权（如果尚未挂载）来挂载RamFS。

### 验证Alluxio集群是否在运行

要验证Alluxio是否正在运行，请访问`http://<alluxio_master_hostname>:19999`以查看Alluxio master的状态页面。

Alluxio带有一个简单的程序可以在Alluxio中读写示例文件。 
使用以下命令运行示例程序：

```console
$ ./bin/alluxio runTests
```

## 常用操作

以下是在Alluxio集群上执行的常见操作。

### 停止Alluxio

停止一个Alluxio服务，运行：

```console
$ ./bin/alluxio-stop.sh all
```

这将停止`conf/workers`和`conf/masters`中列出的所有节点上的所有进程。

可以使用以下命令仅停止master和workers：

```console
$ ./bin/alluxio-stop.sh masters # 停止所有conf/masters的masters
$ ./bin/alluxio-stop.sh workers # 停止所有conf/workers的workers
```

如果不想使用ssh登录所有节点来停止所有进程，可以在每个节点上运行命令以停止每个组件。
对于任何节点，可以使用以下命令停止master或worker：

```console
$ ./bin/alluxio-stop.sh master # 停止本地master
$ ./bin/alluxio-stop.sh worker # 停止本地worker
```

### 重新启动Alluxio

与启动Alluxio类似。 如果已经配置了`conf/workers`和`conf/masters`，可以使用以下命令启动集群：

```console
$ ./bin/alluxio-start.sh all
```

可以使用以下命令仅启动masters或workers：

```console
$ ./bin/alluxio-start.sh masters # 启动conf/masters中全部的master
$ ./bin/alluxio-start.sh workers # 启动conf/workers中全部的worker
```

如果不想使用`ssh`登录所有节点来启动所有进程，可以在每个节点上运行命令以启动每个组件。 对于任何节点，可以使用以下命令启动master或worker：

```console
$ ./bin/alluxio-start.sh master # 启动本地master
$ ./bin/alluxio-start.sh worker # 启动本地worker
```

### 格式化日志


在任何master节点上，运行以下命令格式化Alluxio日志：

```console
$ ./bin/alluxio formatMaster
```

格式化日记将删除Alluxio中的所有元数据。 但是，将不会触及底层存储的数据。

### 动态添加/减少worker

动态添加worker到Alluxio集群就像通过适当配置启动新Alluxio worker进程一样简单。
在大多数情况下，新worker配置应与所有其他worker配置相同。
在新worker上运行以下命令，以将其添加到集群。

```console
$ ./bin/alluxio-start.sh worker SudoMount # 启动本地 worker
```

一旦worker启动，它将在Alluxio master上注册，并成为Alluxio集群的一部分。

减少worker只需要简单停止一个worker进程。

```console
$ ./bin/alluxio-stop.sh worker # 停止本地 worker
```

一旦worker被停止，master将在预定的超时值（通过master参数`alluxio.master.worker.timeout`配置）后将此worker标记为缺失。 主机视worker为“丢失”，并且不再将其包括在集群中。

### 更新master配置

为了更新master配置，必须首先停止服务，更新master节点上的`conf/alluxio-site.properties`文件，并将文件复制到所有节点（例如，使用`bin/alluxio copyDir conf/`），[然后重新启动服务](#restart-alluxio)。

### 更新worker配置

如果只需要为worker节点更新某些本地配置（例如，更改分配给该worker的存储容量或更新存储路径），则无需停止并重新启动master节点。
可以只停止本地worker，更新此worker上的配置（例如`conf/alluxio-site.properties`）文件，然后重新启动此worker。
