---
layout: global
title: 在Virtual Box上运行Tachyon
nickname: 在Virtual Box上运行Tachyon
group: User Guide
priority: 2
---
通过Tachyon自带的[Vagrant脚本](https://github.com/amplab/tachyon/tree/master/deploy/vagrant)，你可以将Tachyon部署在本地机器上的[VirtualBox](https://www.virtualbox.org/)中。该脚本允许你创建，配置以及销毁集群，该集群自动配置了HDFS相关项。

# 前期准备

**安装VirtualBox**

下载[VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**安装Vagrant**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

**安装Tachyon**

下载Tachyon到本地，并解压：

{% include Running-Tachyon-on-Virtual-Box/download-tachyon.md %}

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3.

进入`deploy/vagrant`目录下，运行：

{% include Running-Tachyon-on-Virtual-Box/install-vagrant.md %}

另外，你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，之后进入`deploy/vagrant`目录，运行：

{% include Running-Tachyon-on-Virtual-Box/install-pip.md %}

# 启动集群

现在你可以以Hadoop2.4.1为底层文件系统启动Tachyon集群了，在`deploy/vagrant`目录下运行：

{% include Running-Tachyon-on-Virtual-Box/launch-cluster.md %}

集群中的每个节点运行一个Tachyon worker，`TachyonMaster`节点上运行Tachyon master.

# 访问cluster

**通过Web UI访问**

命令`./create <number of machines> vb`运行成功后，在shell中会输出类似下面的两条语句。

{% include Running-Tachyon-on-Virtual-Box/shell-output.md %}

Tachyon Web UI的默认端口为**19999**。

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

**通过ssh访问**

节点的名称依次被设置成`TachyonMaster`, `TachyonWorker1`, `TachyonWorker2`等等。

通过ssh登陆一个节点，运行：

{% include Running-Tachyon-on-Virtual-Box/ssh.md %}

例如，通过以下命令可以登陆`TachyonMaster`节点：

{% include Running-Tachyon-on-Virtual-Box/ssh-TachyonMaster.md %}

所有的软件都安装在根目录下，例如Tachyon安装在`/tachyon`，Hadoop安装在`/hadoop`。

在`TachyonMaster`节点上，可以对Tachyon运行测试检测其健康状态：

{% include Running-Tachyon-on-Virtual-Box/runTests.md %}

在所有测试完成后，再次访问Tachyon的web UI `http://{MASTER_IP}:19999`，在导航栏中点击`Browse File System`，你应该能看到测试过程中写入到Tachyon的文件。

在集群中的某个节点上，可以通过ssh免密码登陆到集群中的其他节点：

{% include Running-Tachyon-on-Virtual-Box/ssh-other-node.md %}

# 销毁集群

在`deploy/vagrant`目录下运行：

{% include Running-Tachyon-on-Virtual-Box/destroy.md %}

从而销毁之前创建的集群。一次只能创建一个集群。当该命令成功执行后，虚拟机将终止运行。
