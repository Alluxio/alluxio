---
layout: global
title: 本地运行Alluxio
nickname: 本地机器上运行Alluxio
group: Install Alluxio
priority: 1
---

* Table of Contents
{:toc}

## 前提条件

这部分的前提条件是你安装了[Java](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)(JDK 8或更高版本)。

下载 [Alluxio](https://alluxio.io/download) 二进制发行版。

在独立模式下运行，请执行以下操作：

通过拷贝`conf/alluxio-site.properties.template`文件，创建`conf/alluxio-site.properties`。

* 将`conf/alluxio-site.properties`中的`alluxio.master.hostname`设置为`localhost`（即`alluxio.master.hostname=localhost`）。

* 将`conf/alluxio-site.properties`中的`alluxio.master.mount.table.root.ufs`设置为一个本地文件系统上的临时文件夹（例如，`alluxio.master.mount.table.root.ufs=/tmp`）。

* 开启远程登录服务，确保`ssh localhost`能成功。为了避免重复输入密码，你可以将本机的ssh公钥添加到`~/.ssh/authorized_keys`文件中。更多细节请参考[该指南](http://www.linuxproblem.org/art_9.html)。
## 挂载RAMFS文件系统
运行以下命令以挂载RAMFS文件系统。

```console
$ ./bin/alluxio-mount.sh SudoMount
```

## 格式化Alluxio文件系统

> 注意：这个步骤只有在第一次运行Alluxio系统时才需要执行。
> 如果用户在已部署好的Alluxio集群上运行格式化命令，
> 当前服务器上之前保存的Alluxio文件系统的所有数据和元数据都会被清除。
> 但是，底层数据不会改变。

```console
$ ./bin/alluxio format
```

## 本地启动Alluxio文件系统

简单运行如下的命令来启动Alluxio文件系统。

```console
# 如果您尚未挂载ramdisk或要重新挂载（如为了改变ramdisk大小）
$ ./bin/alluxio-start.sh local SudoMount
# 或者，如果已经安装了ramdisk
$ ./bin/alluxio-start.sh local
```

> 注意：用户在linux系统下运行上述命令需要输入密码来获取sudo权限,
> 以便启动RAMFS。如果用户不想每次运行命令输入密码，
> 或者没有sudo权限，可以使用[常见问题]({{ '/cn/overview/FAQ.html' | relativize_url }})中介绍的其他方法。

## 验证Alluxio是否运行

为了确认Alluxio处于运行状态，用户可以访问 **[http://localhost:19999](http://localhost:19999)**，或者查看`logs`文件夹下的日志。

运行一个更全面的系统完整性检查：

```console
$ ./bin/alluxio runTests
```

可以在任意时刻执行以下命令以关闭Alluxio:

```console
$ ./bin/alluxio-stop.sh local
```


# FAQ

## 为什么在linux上运行Alluxio需要sudo权限？

默认情况下，Alluxio使用[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)
存储内存数据。用户在MacOS系统下可以挂载ramfs，不需要超级用户身份。然而，在linux系统下，用户运行"mount"命令（以及 "umount", "mkdir" 和 "chmod" 命令）需要sudo权限。

## 用户没有sudo权限，仍然可以在linux下使用Alluxio么？

假设用户没有sudo权限，那么必须有一个RAMFS（例如`/path/to/ramdisk`）已经被系统管理员挂载，并且用户对此RAMFS有读写权限。你可以在`conf/alluxio-site
.properties`文件中指定该路径：

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/path/to/ramdisk
```

然后在不需要请求root权限的情况下启动Alluxio，使用上述的目录作为存储器：

```console
$ ./bin/alluxio-start.sh local NoMount
```

另外，用户可以使用Linux [tmpFS](https://en.wikipedia.org/wiki/Tmpfs)存储数据，
Tmpfs是一个由内存支持的临时文件夹（e.g.,常见的Linux下的 `/dev/shm`），但会使用交换空间，
因此相比于使用RAMFS，Tmpfs提供的性能会稍差。和使用预先挂载的RAMFS类似，用户可以通过`conf/alluxio-site.properties`配置项配置Tmpfs文件夹

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/dev/shm
```

其次是：

```console
$ ./bin/alluxio-start.sh local NoMount
```

## 我怎样避免通过输入密码运行sudo命令？

选项：

* 通过超级用户身份启动Alluxio。
* 在[sudoers](https://help.ubuntu.com/community/Sudoers)中增加启动Alluxio的用户。
* 在Linux文件 `/etc/sudoers`下添加下面一行，赋予当前用户（e.g., "alluxio"）有限的sudo权限
```
alluxio ALL=(ALL) NOPASSWD: /bin/mount * /mnt/ramdisk, /bin/umount * /mnt/ramdisk, /bin/mkdir * /mnt/ramdisk, /bin/chmod * /mnt/ramdisk
```
这允许"alluxio"用户应用sudo权限在一个具体路径`/mnt/ramdisk` 下执行命令mount, umount, mkdir 和 chmod （假设命令在 `/bin/`）
，并且不需要输入密码。
查看更多关于 [Sudoer User Specifications](https://help.ubuntu.com/community/Sudoers#User_Specifications)的介绍。





