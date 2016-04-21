---
layout: global
title: 本地运行Alluxio
nickname: 本地机器上运行Alluxio
group: User Guide
priority: 1
---

# 单机上独立运行Alluxio

这部分的前提条件是你安装了[Java](Java-Setup.html)(JDK 7或更高版本)。

下载Alluxio二进制发行版{{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

执行Alluxio运行脚本前，在`conf/alluxio-env.sh`中指定必要的环境变量，可以从自带的模板文件中拷贝：

{% include Running-Alluxio-Locally/bootstrap.md %}

在独立模式下运行，确保：

* `conf/alluxio-env.sh`中的`ALLUXIO_UNDERFS_ADDRESS`设置成本地文件系统的临时目录：（例如，`export ALLUXIO_UNDERFS_ADDRESS=/tmp`）。

* 远程登录服务开启，`ssh localhost`能成功。

接着，格式化Alluxio文件系统并启动。*注意：因为Alluxio需要创建
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)，启动
Alluxio会要求用户输入root密码。如果不想重复输入root密码，将主机的公开ssh key添加
到`~/.ssh/authorized_keys`。访问[该指南](http://www.linuxproblem.org/art_9.html)获取更多信息。*

{% include Running-Alluxio-Locally/Alluxio-format-start.md %}

验证Alluxio是否运行，访问**[http://localhost:19999](http://localhost:19999)**，或查看`logs`文件夹下的
日志。也可以运行一个样例程序：

{% include Running-Alluxio-Locally/run-sample.md %}

对于首个样例程序，你应该能够看到如下输出：

{% include Running-Alluxio-Locally/first-sample-output.md %}

再次访问Alluxio Web UI **[http://localhost:19999](http://localhost:19999)**。点击导航栏
`Browse File System`可以看见样例程序写入Alluxio的文件。


运行一个更全面的系统完整性检查：

{% include Running-Alluxio-Locally/run-tests.md %}

可以在任意时刻执行以下命令以关闭Alluxio:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
