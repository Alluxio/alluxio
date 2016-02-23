---
layout: global
title: 在EC2上使用Mesos运行Alluxio
nickname: 在EC2上使用Mesos运行Alluxio
group: User Guide
priority: 4
---

使用Alluxio自带的[Vagrant脚本](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant)可以通过
Mesos将Alluxio部署在Amazon EC2上。该脚本允许你创建，配置以及销毁集群，该集群自动配置了HDFS相关项。

# 前期准备

**安装Vagrant和AWS插件**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

安装AWS Vagrant插件：

{% include Running-Alluxio-on-EC2-Mesos/install-aws-vagrant-plugin.md %}

**安装Alluxio**

下载Alluxio到本地，并解压：

{% include Common-Commands/download-alluxio.md %}

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3。

进入`deploy/vagrant`目录下，运行：

{% include Running-Alluxio-on-EC2-Mesos/install-vagrant.md %}

另外，你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，之后进入`deploy/vagrant`目录，运行：

{% include Running-Alluxio-on-EC2-Mesos/install-pip.md %}

# 启动集群

要在EC2上运行Alluxio集群，首先在[Amazon Web Services site](http://aws.amazon.com/)注册一个Amazon EC2帐号。

接着创建[access keys](https://aws.amazon.com/developers/access-keys/)并且设置`AWS_ACCESS_KEY_ID`和
`AWS_SECRET_ACCESS_KEY`环境变量:

{% include Running-Alluxio-on-EC2-Mesos/access-key.md %}

接着生成EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)。确保将私钥文件的权限设置成只对你可读。

{% include Running-Alluxio-on-EC2-Mesos/generate-key-pair.md %}

在`deploy/vagrant/conf/ec2.yml`配置文件中，将`Keypair`设置为你的keypair名，`Key_Path`设置成pem key路径。

Vagrant脚本默认会在
[该区域(**us-east-1**)和可用区域(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html)
中创建一个名为*alluxio-vagrant-test*的
[安全组](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)。
该安全组会在区域中自动建立，并且所有inbound及outbound网络流量都将打开。你可以在`ec2.yml`配置文件中设
置*security group*、*region*以及*availability zone*的值。

在`deploy/vagrant/conf/mesos.yml`配置文件中，根据你需要从Github分支还是发布版本构建Mesos，将`Type`的值
设置为`Github`或`Release`。

现在你可以启动Mesos集群以及Alluxio Mesos框架，该框架自动在us-east-1b里以Hadoop 2.4.1为底层文件系统启动一
个Alluxio集群。在`deploy/vagrant`运行以下命令：

{% include Running-Alluxio-on-EC2-Mesos/launch-cluster.md %}

# 访问集群

**通过Web UI访问**

命令`./create <number of machines> aws`运行成功后，在shell中会输出类似下面的两条语句。

{% include Running-Alluxio-on-EC2-Mesos/shell-output.md %}

Alluxio Web UI的默认端口为**19999**。

Mesos Web UI的默认端口为**50050**.

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

注意：Alluxio Mesos框架不确保在集群中的alluxioMaster节点启动Alluxio master服务，通过Mesos Web UI找出运
行该服务的节点。

也能通过[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1)监视其状态。

**通过ssh访问**

节点的名称依次被设置成`AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2`等等。

通过ssh登陆一个节点，运行：

{% include Running-Alluxio-on-EC2-Mesos/ssh.md %}

例如，通过以下命令可以登陆`AlluxioMaster`节点：

{% include Running-Alluxio-on-EC2-Mesos/ssh-AlluxioMaster.md %}

所有的软件都安装在根目录下，例如Alluxio安装在`/alluxio`，Hadoop安装在`/hadoop`，Mesos安装在`/mesos`。

可以对Alluxio运行测试检测其健康状态：

{% include Running-Alluxio-on-EC2-Mesos/runTests.md %}

在所有测试完成后，再次访问Alluxio的web UI `http://{MASTER_IP}:19999`，在导航栏中点击
`Browse File System`，你应该能看到测试过程中写入到Alluxio的文件。

# 销毁集群

在`deploy/vagrant`目录下运行：

{% include Running-Alluxio-on-EC2-Mesos/destroy.md %}

从而销毁之前创建的集群。一次只能创建一个集群。当该命令成功执行后，EC2实例将终止运行。
