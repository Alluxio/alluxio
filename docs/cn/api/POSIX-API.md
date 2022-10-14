---
layout: global
title: FUSE基础上的POSIX API
nickname: POSIX API
group: Client APIs
priority: 3
---

* 内容列表
{:toc}

Alluxio-FUSE可以在一台Unix机器上的本地文件系统中挂载一个Alluxio分布式文件系统。通过使用该特性，一些标准的命令行工具（例如`ls`、 `cat`以及`echo`）可以直接访问Alluxio分布式文件系统中的数据。此外更重要的是用不同语言实现的应用程序如C, C++, Python, Ruby, Perl, Java都可以通过标准的POSIX接口(例如`open, write, read`)来读写Alluxio，而不需要任何Alluxio的客户端整合与设置。

Alluxio-FUSE是基于[FUSE](http://fuse.sourceforge.net/)这个项目，并且都支持大多数的文件系统操作。但是由于Alluxio固有的属性，例如它的一次写不可改变的文件数据模型，该挂载的文件系统与POSIX标准不完全一致，尚有一定的局限性。因此，请先阅读[局限性](#局限性)，从而了解该特性的作用以及局限。

## 安装要求

* JDK 1.8及以上
* Linux系统现在支持[libfuse](https://github.com/libfuse/libfuse)的2或者3版本
  * 要使用libfuse2，安装2.9.3及以上(2.8.3也能够工作，但会提示一些警告)
  * 要使用libfuse3，安装3.2.6及以上(目前系统使用3.2.6版本进行测试)
  * 查看[选择libfuse的版本](#%E9%80%89%E6%8B%A9libfuse%E7%9A%84%E7%89%88%E6%9C%AC)来了解和配置要使用的libfuse的版本
* MAC系统上[osxfuse](https://osxfuse.github.io/) 3.7.1及以上

## 用法

### 挂载Alluxio-FUSE

在完成配置以及启动Alluxio集群后，在需要挂载Alluxio的节点上启动Shell并进入`$ALLUXIO_HOME`目录，再运行

```console
$ integration/fuse/bin/alluxio-fuse mount mount_point [alluxio_path]
```

该命令会启动一个后台java进程，用于将对应的Alluxio路径挂载到`<mount_point>`指定的路径。比如，以下这个命令将Alluxio路径`/people`挂载到本地文件系统的`/mnt/people`目录下。

```console
$ integration/fuse/bin/alluxio-fuse mount /mnt/people /people
Starting alluxio-fuse on local host.
Alluxio-fuse mounted at /mnt/people. See /lib/alluxio/logs/fuse.log for logs
```

当`alluxio_path`没有给定时，Alluxio-FUSE会默认挂载到Alluxio根目录下(`/`)。注意`<mount_point>`必须是本地文件系统中的一个空文件夹，并且启动Alluxio-FUSE进程的用户拥有该挂载点及对其的读写权限。你可以多次调用该命令来将Alluxio挂载到不同的本地目录下。所有的Alluxio-FUSE会共享`$ALLUXIO_HOME\logs\fuse.log`这个日志文件。这个日志文件对于错误排查很有帮助。

### 卸载Alluxio-FUSE

要卸载Alluxio-FUSE时，在该节点上启动Shell并进入`$ALLUXIO_HOME`目录，再运行：

```console
$ integration/fuse/bin/alluxio-fuse umount mount_point
```

该命令将终止alluxio-fuse java后台进程，并卸载该文件系统。例如：

```console
$ integration/fuse/bin/alluxio-fuse umount /mnt/people
Unmount fuse at /mnt/people (PID: 97626).
```

默认情况下，如果有任何读写操作未完成，`unmount` 操作会等待最多 120s 。如果 120s 后读写操作仍未完成，那么 Fuse 进程会被强行结束，这会导致正在读写的文件失败，你可以添加 `-s` 参数来避免 Fuse 进程被强行结束。例如：

```console
$ ${ALLUXIO_HOME}/integration/fuse/bin/alluxio-fuse unmount -s /mnt/people
```

### 检查Alluxio-FUSE是否在运行

要罗列所有的挂载点，在该节点上启动Shell并进入`$ALLUXIO_HOME`目录，再运行：

```console
$ integration/fuse/bin/alluxio-fuse stat
```

改命令会输出包括`pid, mount_point, alluxio_path`在内的信息.

例如输出可以是一下格式:

```console
$ pid	mount_point	alluxio_path
80846	/mnt/people	/people
80847	/mnt/sales	/sales
```

## Fuse Shell 工具

Alluxio-FUSE 提供了 Fuse shell 工具对客户端进行一些内部操作， 例如清理 Fuse Client 端的元数据缓存，获取元数据缓存大小等。
Fuse Shell 工具使用 UNIX 系统的标准的命令行 `ls -l` 触发，假如我们的 Alluxio-Fuse 挂载点为 `/mnt/alluxio-fuse`， Fuse Shell 的命令格式为：
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.[COMMAND].[SUBCOMMAND]
```
其中， `/.alluxiocli` 为 Fuse Shell 的标识字符串，`COMMAND` 为 Fuse Shell 命令(例如 `metadatacache` ), `SUBCOMMAND` 为子命令(例如 `drop, size, dropAll` 等)。
目前 Fuse Shell 还只支持 `metadatacache` 命令， 后续我们会扩展更多的命令和交互方式。需要注意，使用 Fuse Shell 工具需要将配置项 `alluxio.fuse.special.command.enabled` 设置为 true。
```console
$ alluxio.fuse.special.command.enabled=true
```

### Metadatacache 命令

我们在使用 Alluxio fuse 时通常会开启客户端元数据缓存，比如在 AI 这种大量小文件读的业务场景下， 开启客户端元数据缓存能够缓解
Alluxio Master 的元数据压力， 提升业务的读性能， 当 Alluxio 中的数据更新后，client 端的元数据缓存也需要更新，通常我们要等待 `alluxio.user.metadata.cache.expiration.time` 配置的超时时间让元数据缓存失效，
这意味着有一个时间窗口元数据可能是无效的，这种情况下推荐使用 Fuse Shell 的 `metadatacache` 命令及时对客户端元数据缓存进行清理, ` metadatacache` 命令格式为：
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.[dropAll|drop|size]
```

#### 使用示例:

- 清理客户端所有的元数据缓存：
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.dropAll
```
- 清理挂载点下某一路径及其父目录缓存：
```console
$ ls -l /mnt/alluxio-fuse/dir/dir1/.alluxiocli.metadatacache.drop
```
上述命令会清除 `/mnt/alluxio-fuse/dir/dir1` 的元数据以及所有父目录的元数据缓存。
- 获取客户端元数据大小
```console
$ ls -l /mnt/alluxio-fuse/.alluxiocli.metadatacache.size
```
你将在下面输出结果中的文件大小字段得到元数据缓存的大小：
```
---------- 1 root root 13 Jan  1  1970 /mnt/alluxio-fuse/.alluxiocli.metadatacache.size
```

## 可选配置

Alluxio-FUSE是基于标准的alluxio-core-client-fs进行操作的。你也许希望像使用其他应用的client一样，自定义该alluxio-core-client-fs的行为。

一种方法是编辑`$ALLUXIO_HOME/conf/alluxio-site.properties`配置文件来更改客户端选项。注意所有的更改应该在Alluxio-FUSE启动之前完成。

### 选择libfuse的版本

Alluxio现在支持libfuse2或者libfuse3。Alluxio的libfuse2的支持较成熟和稳定，已在生产环境中大规模部署。alluxio目前试验性支持libfuse3。Alluxio未来的开发也将专注于libfuse3，更好地利用libfuse3提供的新特性。

如果系统内只安装了一个libfuse版本，alluxio将会使用这个版本。在大多数发行版中，libfuse2和libfuse3可以同时存在。如果两个版本都安装了，为了保持兼容性，alluxio将会默认使用libfuse2版本。

要手动修改要使用的libfuse版本，在`$ALLUXIO_HOME/conf/alluxio-site.properties`中增加以下配置：

```
alluxio.fuse.jnifuse.libfuse.version=3
```

有效的值为`2`（只使用libfuse2）、`3`（只使用libfuse3）和其他整数值（先尝试加载libfuse2，如果失败了，加载libfuse3）。

查看`logs/fuse.out`来查看当前具体使用的哪个版本。

```
INFO  NativeLibraryLoader - Loaded libjnifuse with libfuse version 2(或者3).
```

## 局限性

目前，Alluxio-FUSE支持大多数基本文件系统的操作。然而，由于Alluxio某些内在的特性，一定要清楚：

* 文件只能顺序地写入一次，并且无法修改;这意味着如果要修改一个文件，你需要先删除改文件，然后再重新创建。例如当目标文件存在时拷贝命令`cp`会失败。
* Alluxio没有hard-link和soft-link的概念，所以不支持与之相关的命令如`ln`。此外关于hard-link的信息也不在`ll`的输出中显示。
* 只有当Alluxio的`alluxio.security.group.mapping.class`选项设置为`ShellBasedUnixGroupsMapping`的值时，文件的用户与分组信息才与Unix系统的用户分组对应。否则`chown`与`chgrp`的操作不生效，而`ll`返回的用户与分组为启动Alluxio-FUSE进程的用户与分组信息。

## 性能考虑

由于FUSE和JNR的配合使用，与直接使用[原生文件系统Java客户端]({{ '/cn/api/Java-API.html' | relativize_url }})相比，使用挂载文件系统的性能会相对较差。

大多数性能问题的原因在于，每次进行`read`或`write`操作时，内存中都存在若干个副本，并且FUSE将写操作的最大粒度设置为128KB。其性能可以利用kernel 3.15引入的FUSE回写(write-backs)缓存策略从而得到大幅提高（但该特性目前尚不被libfuse 2.x用户空间库支持）。

## Alluxio-FUSE配置参数

以下是Alluxio-FUSE相关的配置参数。

<table class="table table-striped">
<tr><th>参数</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.Alluxio-FUSE-parameter %}
  <tr>
    <td>{{ item.parameter }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.Alluxio-FUSE-parameter[item.parameter] }}</td>
  </tr>
{% endfor %}
</table>

## 致谢

该项目使用[jnr-fuse](https://github.com/SerCeMan/jnr-fuse)以支持基于Java的FUSE。
