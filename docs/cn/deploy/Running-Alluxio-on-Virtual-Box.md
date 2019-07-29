---
layout: global
title: 在Virtual Box上运行Alluxio
nickname: 在Virtual Box上运行Alluxio
group: Deploying Alluxio
priority: 2
---

* 内容列表
{:toc}

通过Alluxio自带的[Vagrant脚本](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant)，你可以
将Alluxio部署在本地机器上的[VirtualBox](https://www.virtualbox.org/)中。该脚本允许你创建，配置以及撤销
集群，该集群自动配置了HDFS相关项。

## 前期准备

**安装VirtualBox**

下载[VirtualBox](https://www.virtualbox.org/wiki/Downloads)

**安装Vagrant**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

**安装Alluxio**

克隆 [Alluxio Repository](https://github.com/Alluxio/alluxio)到你本地的机器上。

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3.

进入`deploy/vagrant`目录下，运行：

```console
$ sudo bash bin/install.sh
```

另外，你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，之后进入`deploy/vagrant`目录，运行：

```console
$ sudo pip install -r pip-req.txt
```

## 启动集群

现在你可以以Hadoop2.4.1为底层文件系统启动Alluxio集群了，在`deploy/vagrant`目录下运行：

```console
$ ./create <number of machines> vb
```

集群中的每个节点运行一个Alluxio worker，`AlluxioMaster`节点上运行Alluxio master.

## 访问cluster

**通过Web UI访问**

命令`./create <number of machines> vb`运行成功后，在shell中会输出类似下面的两条语句。

```
>>> AlluxioMaster public IP is xxx, visit xxx:19999 for Alluxio web UI<<<
>>> visit default port of the web UI of what you deployed <<<
```

Alluxio Web UI的默认端口为**19999**。

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

**通过ssh访问**

节点的名称依次被设置成`AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2`等等。

通过ssh登陆一个节点，运行：

```console
$ vagrant ssh <node name>
```

例如，通过以下命令可以登陆`AlluxioMaster`节点：

```console
$ vagrant ssh AlluxioMaster
```

所有的软件都安装在根目录下，例如Alluxio安装在`/alluxio`，Hadoop安装在`/hadoop`。

在`AlluxioMaster`节点上，可以对Alluxio运行测试检测其健康状态：

```console
$ /alluxio/bin/alluxio runTests
```

在所有测试完成后，再次访问Alluxio的web UI `http://{MASTER_IP}:19999`，在导航栏中点
击`Browse File System`，你应该能看到测试过程中写入到Alluxio的文件。

在集群中的某个节点上，可以通过ssh免密码登陆到集群中的其他节点：

```console
$ ssh AlluxioWorker1
```

## 撤销集群

在`deploy/vagrant`目录下运行：

```console
$ ./destroy
```

从而撤销之前创建的集群。一次只能创建一个集群。当该命令成功执行后，虚拟机将终止运行。
