---
layout: global
title: 本地运行Alluxio
nickname: 本地机器上运行Alluxio
group: User Guide
priority: 1
---

# 单机上独立运行Alluxio

开始之前需要安装[Java](Java-Setup.html)(JDK 7或更高版本)。

下载Alluxio二进制发行版{{site.ALLUXIO_RELEASED_VERSION}}:

{% include Running-Alluxio-Locally/download-Alluxio-binary.md %}

执行Alluxio运行脚本前，在`conf/alluxio-env.sh`中指定必要的环境变量，可以从自带的模板文件中拷贝：

{% include Running-Alluxio-Locally/copy-template.md %}

在独立模式下运行，确保：

* `conf/alluxio-env.sh`中的`ALLUXIO_UNDERFS_ADDRESS`设置成本地文件系统的临时目录：（例如，`export ALLUXIO_UNDERFS_ADDRESS=/tmp`）。

* 远程登录服务打开，`ssh localhost`能成功。

接着，格式化Alluxio文件系统并启动。*注意：因为Alluxio需要创建
[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)，启动
Alluxio会要求用户输入root密码。如果不想重复输入root密码，将主机的公开ssh key添加
到`~/.ssh/authorized_keys`.*

{% include Running-Alluxio-Locally/Alluxio-format-start.md %}

验证Alluxio是否运行，访问**[http://localhost:19999](http://localhost:19999)**，或查看`logs`文件夹下的
日志。也可以运行一个样例程序：

{% include Running-Alluxio-Locally/run-sample.md %}

因为是首个样例程序，能够看到如下输出：

{% include Running-Alluxio-Locally/first-sample-output.md %}

再次访问Alluxio Web UI **[http://localhost:19999](http://localhost:19999)**。点击导航栏
`Browse File System`可以看见样例程序写入Alluxio的文件。


运行一个更全面的系统完整性检查：

{% include Running-Alluxio-Locally/run-tests.md %}

可以在任意时刻执行以下脚本停止Alluxio:

{% include Running-Alluxio-Locally/Alluxio-stop.md %}
