---
layout: global
title: 本地运行Alluxio
nickname: 本地机器上运行Alluxio
group: Deploying Alluxio
priority: 1
---

# 前提条件

这部分的前提条件是你安装了[Java](Java-Setup.html)(JDK 7或更高版本)。

下载Alluxio二进制发行版{{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

在独立模式下运行，需要确保：

* 将`conf/alluxio-site.properties`中的`alluxio.master.hostname`设置为`localhost`(即`alluxio.master.hostname=localhost`)。

* 将`conf/alluxio-site.properties`中的`alluxio.underfs.address`设置为一个本地文件系统上的临时文件夹（例如，`alluxio.underfs.address=/tmp`）。

* 开启远程登录服务，确保`ssh localhost`能成功。为了避免重复输入密码，你可以将本机的ssh公钥添加到`~/.ssh/authorized_keys`文件中。更多细节请参考[该指南](http://www.linuxproblem.org/art_9.html)。

# 格式化Alluxio文件系统

> 这个步骤只有在第一次运行Alluxio系统时才需要执行。
> 当前服务器上之前保存的Alluxio文件系统的所有数据和元数据都会被清除。

```bash
$ ./bin/alluxio format
```

# 本地启动Alluxio文件系统

## 以sudo权限启动Alluxio

默认情况下，Alluxio在启动时会创建一个[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)作为自己的内存数据存储。
这个步骤需要sudo权限来执行“mount”，“umount”，“mkdir”和“chmod”等操作。有两种方式来实现：

* 以超级用户的身份启动Alluxio，或者
* 给运行Alluxio的用户（例如“alluxio”）有限的sudo权限。在Linux系统上，可以向`/etc/sudoers`文件添加如下的一行内容来实现：
`alluxio ALL=(ALL) NOPASSWD: /bin/mount * /mnt/ramdisk, /bin/umount * /mnt/ramdisk, /bin/mkdir * /mnt/ramdisk, /bin/chmod * /mnt/ramdisk`
这将允许Linux用户“alluxio”以sudo权限进行mount，umount，在具体路径`/mnt/ramdisk`上执行chmod等操作（假设操作命令在 `/bin/`目录下）而不需要输入密码，但并不给“alluxio”用户赋予其他的sudo权限。
更详细的解释可以参考[Sudoer用户技术说明](https://help.ubuntu.com/community/Sudoers#User_Specifications)。

以适当的用户身份，运行如下的命令来启动Alluxio文件系统。

```bash
$ ./bin/alluxio-start.sh local
```

## 不带sudo权限启动Alluxio

或者，如果一个RAMFS（例如`/path/to/ramdisk`）已经被系统管理员挂载，你可以在`conf/alluxio-site.properties`文件中指定该路径：

```
alluxio.worker.tieredstore.level0.alias=MEM
alluxio.worker.tieredstore.level0.dirs.path=/path/to/ramdisk
```

然后在不需要请求root权限的情况下启动Alluxio：

```bash
$ ./bin/alluxio-start.sh local NoMount
```

## 验证Alluxio是否运行

验证Alluxio是否运行，访问**[http://localhost:19999](http://localhost:19999)**，或查看`logs`文件夹下的
日志。也可以运行一个样例程序：

{% include Running-Alluxio-Locally/run-sample.md %}

对于首个样例程序，你应该能够看到如下输出：

{% include Running-Alluxio-Locally/first-sample-output.md %}

再次访问Alluxio Web UI **[http://localhost:19999](http://localhost:19999)**。点击导航栏
`Browse`可以看见样例程序写入Alluxio的文件。


运行一个更全面的系统完整性检查：

{% include Running-Alluxio-Locally/run-tests.md %}

可以在任意时刻执行以下命令以关闭Alluxio:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
