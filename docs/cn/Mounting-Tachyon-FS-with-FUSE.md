---
layout: global
title: 使用FUSE挂载Tachyon (Beta)
nickname: Tachyon-FUSE
group: Features
priority: 4
---

* Table of Contents
{:toc}

Tachyon-FUSE是一个新的处于实验阶段的特性，该特性允许在一台Linux机器上的本地文件系统中挂载一个Tachyon分布式文件系统。通过使用该特性，标注的工具（例如`ls`、 `cat`以及`echo`）和传统的POSIX应用程序都能够访问Tachyon分布式文件系统中的数据。

由于Tachyon固有的属性，例如它的write-once/read-many-times文件数据模型，该挂载的文件系统并不完全符合POSIX标准，有一定的局限性。因此，在使用该特性之前，请先阅读本页面余下的内容，从而了解该特性的作业以及局限。

# 安装依赖

* Linux kernel 2.6.9及以上
* JDK 1.8及以上
* libfuse 2.9.3及以上
  (2.8.3也能够工作，但会提示一些警告)

# 构建

在编译Tachyon源码过程中，只有当maven的`buildFuse`设置开启时，tachyon-fuse才会被构建。当使用JDK 1.8及以上编译Tachyon源码时该设置会自动开启。

为了保持与JAVA 6和7的兼容性，预编译的tachyon二进制文件并不支持tachyon-fuse，因此若需要在部署中使用tachyon-fuse，你需要自己构建Tachyon。

最好的方式是从Tachyon [GitHub repository](https://github.com/amplab/tachyon)处获取你需要的分支的源码，或者直接从[source distribution](https://github.com/amplab/tachyon/releases)处获取，请参考[该页面](Building-Tachyon-Master-Branch.html)进行构建。

# 用法

## 挂载Tachyon-FUSE

在完成配置以及启动Tachyon集群后，在需要挂载Tachyon的节点上启动Shell并进入`$TACHYON_HOME`目录，再运行

{% include Mounting-Tachyon-FS-with-FUSE/tachyon-fuse-mount.md %}

该命令会启动一个后台java进程，用于将Tachyon挂载到`<mount_point>`指定的路径。注意`<mount_point>`必须是本地文件系统中的一个空文件夹，并且该用户拥有该挂载点及对其的读写权限。另外，目前每个节点上只能挂载一个Tachyon-FUSE。

## 卸载Tachyon-FUSE

要卸载Tachyon-FUSE时，在该节点上启动Shell并进入`$TACHYON_HOME`目录，再运行：

{% include Mounting-Tachyon-FS-with-FUSE/tachyon-fuse-umount.md %}

该命令将终止tachyon-fuse java后台进程，并卸载该文件系统。

## 检查Tachyon-FUSE是否在运行

{% include Mounting-Tachyon-FS-with-FUSE/tachyon-fuse-stat.md %}

## 可选配置

Tachyon-FUSE是基于标准的tachyon-client进行操作的。你也许想像使用其他应用的client一样，自定义该tachyon-client的行为。

一种方法是编辑`$TACHYON_HOME/bin/tachyon-fuse.sh`配置文件，将特定的配置项添加到`TACHYON_JAVA_OPTS`变量中。

# 操作前提和状态

目前，tachyon-fuse支持大多数基本文件系统的操作。然而，由于Tachyon某些内在的特性，一定要清楚：

* 文件只能顺序地写入一次，并且无法修改;
* 由于以上的限制，文件只有只读访问方法。

下面说明作用于文件系统的UNIX系统调用受到的限制条件。

## `open(const char* pathname, int flags, mode_t mode)`
(see also `man 2 open`)

如果`pathname`为一个Tachyon中不存在的文件，那么open操作只有在一下条件满足时才会成功：

1. `pathname`的基目录在Tachyon中存在;
2. `O_CREAT`和`O_WRONLY`被传递到`flags`位字段中。

同样的，当(1)满足并且`pathname`不存在时，`creat(const char* pathname )`操作会成功。

如果`pathname`为一个Tachyon中存在的文件，那么open操作只有当以下条件满足时才会成功：
1. `O_RDONLY`被传递到`flags`位字段中。

注意，无论哪种情况，目前Tachyon-FUSE会忽略`mode`参数。

## `read(int fd, void* buf, size_t count)`
(see also `man 2 read`)

只有当`fd`指向的文件已经在指定`O_RDONLY` flags方式下被打开时，read操作才会成功。

## `lseek(int fd, off_t off, int whence)`
(see also `man 2 lseek`)

Seek操作只支持用于读的文件，即在指定`O_RDONLY` flags方式下被打开的文件。

## `write(int fd, const void* buf, size_t count)`
(see also `man 2 write`)

只有当`fd`指向的文件已经在指定`O_WRONLY` flags方式下被打开时，write操作才会成功。

# 性能考虑

由于FUSE和JNR的配合使用，与直接使用tachyon-client相比，使用挂载文件系统的性能会相当差。也就是说，如果你在乎的更多是性能而不是这个功能，那么不应当使用Tachyon-FUSE。

大多数性能问题的原因在于，每次进行`read`或`write`操作时，内存中都存在若干个副本，并且FUSE将写操作的最大粒度设置为128KB。其性能可以利用kernel 3.15引入的FUSE回写(write-backs)缓存策略从而得到大幅提高（但该特性目前尚不被libfuse 2.x用户空间库支持）。

# Tachyon-FUSE配置参数

以下是Tachyon-FUSE的配置参数。

<table class="table table-striped">
<tr><th>参数</th><th>默认值</th><th>描述</th></tr>
{% for item in site.data.table.Tachyon-FUSE-parameter %}
  <tr>
    <td>{{ item.parameter }}</td>
    <td>{{ item.defaultValue }}</td>
    <td>{{ site.data.table.cn.Tachyon-FUSE-parameter.[item.parameter] }}</td>
  </tr>
{% endfor %}
</table>

# 致谢

该项目使用[jnr-fuse](https://github.com/SerCeMan/jnr-fuse)以支持基于Java的FUSE。
