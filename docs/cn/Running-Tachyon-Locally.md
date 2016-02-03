---
layout: global
title: 本地运行Tachyon
nickname: 本地机器上运行Tachyon
group: User Guide
priority: 1
---

# 单机上独立运行Tachyon

开始之前需要安装[Java](Java-Setup.html)(JDK 7或更高版本)。

下载Tachyon二进制发行版{{site.TACHYON_RELEASED_VERSION}}:

{% include Running-Tachyon-Locally/download-Tachyon-binary.md %}

执行Tachyon运行脚本前，在`conf/tachyon-env.sh`中指定必要的环境变量，可以从自带的模板文件中拷贝：

{% include Running-Tachyon-Locally/copy-template.md %}

在独立模式下运行，确保：

* `conf/tachyon-env.sh`中的`TACHYON_UNDERFS_ADDRESS`设置成本地文件系统的临时目录：（例如，`export TACHYON_UNDERFS_ADDRESS=/tmp`）。

* 远程登录服务打开，`ssh localhost`能成功。

接着，格式化Tachyon文件系统并启动。*注意：因为Tachyon需要创建[RAMFS](https://www.kernel.org/doc/Documentation/filesystems/ramfs-rootfs-initramfs.txt)，启动Tachyon会要求用户输入root密码。如果不想重复输入root密码，将主机的公开ssh key添加到`~/.ssh/authorized_keys`.*

{% include Running-Tachyon-Locally/Tachyon-format-start.md %}

验证Tachyon是否运行，访问**[http://localhost:19999](http://localhost:19999)**，或查看`logs`文件夹下的日志。也可以运行一个样例程序：

{% include Running-Tachyon-Locally/run-sample.md %}

因为是首个样例程序，能够看到如下输出：

{% include Running-Tachyon-Locally/first-sample-output.md %}

再次访问Tachyon Web UI **[http://localhost:19999](http://localhost:19999)**。点击导航栏`Browse File System`可以看见样例程序写入Tachyon的文件。


运行一个更全面的系统完整性检查：

{% include Running-Tachyon-Locally/run-tests.md %}

可以在任意时刻执行以下脚本停止Tachyon:

{% include Running-Tachyon-Locally/tachyon-stop.md %}
