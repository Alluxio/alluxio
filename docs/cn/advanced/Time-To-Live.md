---
layout: global
title: Time to Live
nickname: 生存时间
group: Advanced
priority: 8
---

* 内容列表
{:toc}

在Alluxio中的每个文件和目录上，支持 `生存时间(TTL)` 设置。该特性可以有效地管理Alluxio缓存，特别是数据的访问模式有严格的规律的环境中。
例如，如果只分析在最后一周获取的数据。TTL可用于显式地刷新旧数据以释放缓存以获取新的数据文件。

## 概述

Alluxio具有与每个文件或目录相关联的TTL属性。这些属性通过日志持久化。保证集群重启后的一致性。
当Alluxio运行时，活跃的master节点负责保存元数据在内存中。在内部，master进程运行一个后台线程定期检查文件是否已经到达它对应的TTL值。

注意，后台线程在一个可配置的时间段内运行，默认为1小时。这意味着TTL在下一次检查间隔前不会强制执行，TTL的强制执行可以达到1
TTL间隔延迟。间隔长度由 `alluxio.master.ttl.checker.interval` 属性设置。

例如，将间隔设置为10分钟，将以下内容添加到 `alluxio-site.properties`:

```
alluxio.master.ttl.checker.interval=10m
```

查看[配置页](Configuration-Settings.html)获取有关Alluxio设置的更多配置

虽然master节点负责执行TTL，但是TTL值的设置取决于客户端

## 接口

有三种方法可以设置Alluxio的TTL。
1. 通过Alluxio的shell命令行。
1. 通过Alluxio Java文件系统API。
1. 被动加载元数据或创建文件

TTL API如下:

```
SetTTL(path, duration, action)
`path`          Alluxio命名空间中的路径
`duration`      TTL操作生效之前的毫秒数，将覆盖任何之前的值
`action`        生存时间过后要采取的行动。`FREE`将导致文件被逐出Alluxio存储，不管pin状态如何。
                `DELETE`将导致文件从Alluxio命名空间中删除，并在存储中删除。
                注意:`DELETE`是某些命令的默认值，将导致文件被永久删除。
```

### 使用命令行

查看[命令行文档](Command-Line-Interface.html#setttl).

### Java 文件系统接口

使用Alluxio文件系统对象来设置具有适当选项的文件属性。

```java
FileSystem alluxioFs = FileSystem.Factory.get();

AlluxioURI path = new AlluxioURI("alluxio://hostname:port/file/path");
long ttlMs = 86400000L; // 1 day
TtlAction ttlAction = TtlAction.FREE; // Free the file when TTL is hit

SetAttributeOptions options = SetAttributeOptions.defaults().setTtl(ttlMs).setTtlAction(ttlAction);
alluxioFs.setAttribute(path);
```

查看[Java 文档](https://www.alluxio.org/javadoc/{{site.ALLUXIO_MAJOR_VERSION}}/index.html)获取更多信息

### 被动加载元数据或创建文件

每当一个新文件被添加到Alluxio命名空间时，用户都可以选择被动添加一个TTL到那个文件。这在用户希望访问的文件被暂时使用时
非常有用。它不是多次调用API，而是自动设置为文件发现。

注意:被动TTL更方便，但也不太灵活。选项是客户端级别的，所以所有选项都是
来自客户端的TTL请求，将具有相同的动作和生存时间。

被动TTL使用以下配置选项:

* `alluxio.user.file.load.ttl` - 针对从底层存储加载到Alluxio中的任何新文件的默认生存时间。默认情况下没有TTL。
* `alluxio.user.file.load.ttl.action` - 任何ttl设置的默认操作，设置在从一个under store加载到Alluxio的文件上。
默认情况下是 `DELETE`。
* `alluxio.user.file.create.ttl` - 新创建的任何文件的默认的生存时间。默认情况下没有TTL。
* `alluxio.user.file.create.ttl.action` - 在一个新创建的文件上的任何ttl设置的默认操作。默认情况下是`DELETE`。

有两对选项，一组用于  `load`，一组用于`create`。`load`指的是Alluxio从底层存储获取的文件。
`Create`指创建的新文件或目录。

这两个选项在默认情况下都是禁用的，应该只由具有严格数据访问模式的客户端启用
例如，要删除`runTests`在1分钟后创建的文件:

```
bin/alluxio runTests -Dalluxio.user.file.create.ttl=1m -Dalluxio.user.file.create.ttl.action=DELETE
```

注意，如果您尝试使用这个示例，请确保将 `alluxio.master.ttl.checker.interval` 设置为较短时间，即1分钟。
