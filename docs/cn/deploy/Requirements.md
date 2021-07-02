---
layout: global
title: Alluxio基本要求
nickname: 基本要求
group: Install Alluxio
priority: 9
---

* Table of Contents
{:toc}

## 基本要求

下面是在本地或集群模式下运行Alluxio的基本要求：

* 集群节点需要运行在以下操作系统之一：
  * MacOS 10.10或更高版本
  * CentOS - 6.8 或 7
  * RHEL - 7.x
  * Ubuntu - 16.04
* Alluxio需要JDK 8。 不支持更高版本：
  * Java JDK 8（Oracle或OpenJDK发行版都支持）
* Alluxio仅支持IPv4网络协议
* 开放以下端口和协议
  * Inbound TCP 22 - 以用户身份ssh进入指定节点上安装Alluxio组件。

### Master要求

下面是运行Alluxio Master进程的集群节点所需要的配置。

注意这些是运行最低要求。 
大规模高负载下运行Alluxio相应系统要求会随之增加。

* 最少4 GB硬盘空间
* 最少4 GB内存
* 最少4个CPU核
* 开放以下端口和协议：
  * Inbound TCP 19998-Alluxio master的默认RPC端口
  * Inbound TCP 19999-Alluxio master的默认web UI端口：`http://<master-hostname>:19999`
  * Inbound TCP 20001-Alluxio job master的默认RPC端口
  * Inbound TCP 20002-Alluxio job master的默认网络UI端口
  * Embedded Journal要求
    * Inbound TCP 19200-Alluxio master用于内部leader选举的默认端口
    * Inbound TCP 20003-Alluxio job master用于内部leader选举的默认端口

### Worker要求

下面是运行Alluxio Worker进程的集群节点所需要的配置。

* 最小1 GB硬盘空间
* 最少1 GB内存
* 最少2个CPU核
* 开放以下端口和协议：
  * Inbound TCP 29999-Alluxio worker的默认RPC端口
  * Inbound TCP 30000-Alluxio worker的默认网络UI端口：`http://<worker-hostname>:30000`
  * Inbound TCP 30001-Alluxio job worker的默认RPC端口
  * Inbound TCP 30002-Alluxio job worker的默认数据端口
  * Inbound TCP 30003-Alluxio job worker的默认网络UI端口：`http://<worker-hostname>:30003`

#### Worker Cache

需要为Alluxio Workers配置作为缓存的存储空间。 
默认情况下Alluxio为Worker提供一个[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)，但是可以对其进行修改以使用其他存储卷的。 
通过在`alluxio.worker.tieredstore.level％d.dirs.path`中提供其他目录，用户可以指定Alluxio使用不同于默认配置的存储介质和目录。 
对于希望一开始使用默认配值的用户，使用任何sudo权限帐户运行命令`./bin/alluxio-mount.sh SudoMount worker`。 
注意上述命令应在完成`alluxio-site.properties`文件中设置`alluxio.worker.ramdisk.size`并将所有workers添加到`conf/workers`文件后运行。

```console
$ ./bin/alluxio-mount.sh SudoMount workers
```

### Proxy要求

Proxy进程提供一个基于REST的客户端，需要：

* 最少1 GB内存
* 开放以下端口和协议：
  * Inbound TCP 39999- clients用来访问Proxy节点。

### Fuse要求

下面是Alluxio针对运行fuse进程节点要求

注意这些是运行Alluxio软件最低要求。 
大规模负载下运行Alluxio Fuse会增加系统要求。

* 最少1个CPU核
* 最少1 GB内存
* 已安装Fuse
  * libfuse 2.9.3或更高版本（适用于Linux）
  * osxfuse 3.7.1或更高版本（适用于MacOS）

## 其他要求

Alluxio还可以将日志汇总到一个远程服务器中以便统一查看。 
以下是Logging Server的端口和资源要求。
 
### Remote Logging Server要求

下面是Alluxio针对运行Remote Logging Server要求:

* 最少1 GB硬盘空间
* 最少1 GB内存
* 最少2个CPU核
* 开放以下端口和协议：
  * Inbound TCP 45600 - 以便日志程序将日志写入服务器。
