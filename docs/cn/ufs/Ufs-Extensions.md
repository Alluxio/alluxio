---
layout: global
title: 底层存储扩展
group: Storage Integrations
priority: 100
---

* 内容列表
{:toc}

这篇文档是帮助用户进行文件系统的扩展，请查看[创建新Alluxio底层存储系统文档]({{ '/cn/ufs/Ufs-Extension-API.html' | relativize_url }})来阅读扩展开发文档。

Alluxio可以在运行时扩展额外的底层存储模块，底层存储模块的扩展（通过JARs包进行编译）可以被包括在一个Alluxio Core中具体的位置，
不需要重启运行的进程。对于没有现有原生支持的存储系统，添加与Alluxio相连的底层存储模块可以使其与Alluxio一起工作。

# 扩展列表

以下是底层存储扩展计划的列表：

- [S3N](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/s3n)
- [GlusterFS](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/glusterfs)
- [OBS](https://github.com/Alluxio/alluxio-extensions/tree/master/underfs/obs)

# 管理扩展

扩展JARs包通过扩展目录配置项`alluxio.extensions.dir` (默认: `${alluxio-home}/extensions`)获取，扩展命令行实用程序用来管理
Alluxio集群的扩展JAR包的分布。在CLI不可用的环境下（参考下面的配置），JAR包被放置在扩展目录中。例如，当在容器中运行时，在合适的位置
通过额外的二进制文件创建一个客户镜像。

## 命令行实用程序

命令行实用程序是用来提供帮助扩展管理。

```console
$ ./bin/alluxio extensions
Usage: alluxio extensions [generic options]
	 [install <URI>]
	 [ls]
	 [uninstall <JAR>]
```

### 安装

`install`命令通过命令`rsync`和`ssh`，向`conf/masters` 和`conf/workers`列出的Alluxio服务器拷贝提供的JAR。然而，这些工具可能不是在所有的
环境中适用。在脚本中，使用其他更多的可用工具来将JAR放置在Alluxio服务器配置项`alluxio.extensions.dir`中的位置。

### 列举

为了列出任意给定的运行Alluxio进程的主机所安装的扩展，你可以用`ls`命令。实用程序通过扫描本地扩展目录列出所有安装的扩展。

### 卸载

`uninstall`命令和`install`命令类似，处理所有`conf/masters`和`conf/workers`包括的主机。手动移除Alluxio服务器中的扩展，
防止一些主机是运行命令行的主机不可达的。

### 从Maven坐标中安装

为了从maven中安装，按照下面的步骤下载JAR,并安装。

```console
$ mvn dependency:get -DremoteRepositories=http://repo1.maven.org/maven2/ -DgroupId=<extension-group> \
 -DartifactId=<extension-artifact> -Dversion=<version> -Dtransitive=false -Ddest=<extension>.jar

$ ./bin/alluxio extensions install <extension.jar>
```

# 验证

一旦扩展JAR部署之后，你应该可以通过如下Alluxio CLI命令挂载你的底层存储。

```console
$ ./bin/alluxio fs mount /my-storage <scheme>://<path>/ --option <key>=<value>
```
其中，`<key>=<value>`可以被底层存储的任何必需配置替代。

执行完整的测试：

```console
$ ./bin/alluxio runTests --directory /my-storage
```
