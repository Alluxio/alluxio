---
layout: global
title: 在EC2上运行带容错机制的Tachyon
nickname: 运行带容错机制的Tachyon
group: User Guide
priority: 4
---

使用Tachyon自带的[Vagrant脚本](https://github.com/amplab/tachyon/tree/master/deploy/vagrant)可以将带容错机制的Tachyon部署在Amazon EC2上。该脚本允许你创建，配置以及销毁集群，该集群自动配置了HDFS相关项。

# 前期准备

**安装Vagrant和AWS插件**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

安装AWS Vagrant插件：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/install-aws-vagrant-plugin.md %}

**安装Tachyon**

下载Tachyon到本地，并解压：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/download-tachyon.md %}

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3。

进入`deploy/vagrant`目录下，运行：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/install-vagrant.md %}

另外，你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，之后进入`deploy/vagrant`目录，运行：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/install-pip.md %}


# 启动集群

要在EC2上运行Tachyon集群，首先在[Amazon Web Services site](http://aws.amazon.com/)注册一个Amazon EC2帐号。

接着创建[access keys](https://aws.amazon.com/developers/access-keys/)并且设置`AWS_ACCESS_KEY_ID`和`AWS_SECRET_ACCESS_KEY`环境变量:

{% include Running-Tachyon-Fault-Tolerant-on-EC2/access-keys.md %}

接着生成EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)。确保将私钥文件的权限设置成只对你可读。

{% include Running-Tachyon-Fault-Tolerant-on-EC2/generate-key-pairs.md %}

复制`deploy/vagrant/conf/ec2.yml.template`到`deploy/vagrant/conf/ec2.yml`：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/copy-ec2-yml.md %}

在`deploy/vagrant/conf/ec2.yml`配置文件中，将`Keypair`设置为你的keypair名，`Key_Path`设置成pem key路径。

在`deploy/vagrant/conf/tachyon.yml`配置文件中，将`Masters`设置为你想要的TachyonMasters的数量，在容错模式下，`Masters`的值应该大于1。

Vagrant脚本默认会在[该区域(**us-east-1**)和可用区域(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html)中创建一个名为*tachyon-vagrant-test*的[安全组](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)。
该安全组会在区域中自动建立，并且所有inbound及outbound网络流量都将打开。你可以在`ec2.yml`配置文件中设置*security group*、*region*以及*availability zone*的值。

现在你可以以Hadoop2.4.1为底层文件系统，在us-east-1a下启动Tachyon集群了，在`deploy/vagrant`目录下运行：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/launch-cluster.md %}

注意`<number of machines>`的值应该大于等于`deploy/vagrant/conf/tachyon.yml`配置文件中设置的`Masters`的值。

集群中每个节点都运行一个Tachyon worker，每个master节点都运行一个Tachyon master，而leader为这些master节点中的其中之一。

# 访问集群

**通过Web UI访问**

命令`./create <number of machines> aws`运行成功后，在shell中会输出类似下面的三条语句：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/shell-output.md %}

第一行为Tachyon master leader的公共IP。

第二行为其他软件如Hadoop的master的公共IP。

Tachyon Web UI的默认端口为**19999**。

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

也能通过[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1)监视其状态。

**通过ssh访问**

所有创建的节点分为两类。

一类包含`TachyonMaster`、`TachyonMaster2`等等，即所有的Tachyon master，其中之一为leader，其余为备用节点。`TachyonMaster`也是其他软件如Hadoop的master节点。这些节点同时也运行着Tachyon以及其他软件如Hadoop的worker进程。

另一类包含`TachyonWorker1`、`TachyonWorker2`等等，这些节点运行着Tachyon以及其他软件如Hadoop的worker进程。

通过ssh登陆一个节点，运行：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/ssh.md %}

例如，通过以下命令可以登陆`TachyonMaster`节点：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/ssh-TachyonMaster.md %}

所有的软件都安装在根目录下，例如Tachyon安装在`/tachyon`，Hadoop安装在`/hadoop`，Zookeeper安装在`/zookeeper`。

在leader节点上，可以对Tachyon运行测试检测其健康状态：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/runTests.md %}

在所有测试完成后，再次访问Tachyon的web UI `http://{MASTER_IP}:19999`，在导航栏中点击`Browse File System`，你应该能看到测试过程中写入到Tachyon的文件。

通过ssh可以登陆到当前Tachyon master leader，并查找TachyonMaster进程的进程ID：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/jps.md %}

然后将该leader进程终止掉：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/kill-leader.md %}

接着，为了找到新的leader，通过ssh登陆到`TachyonMaster`节点，该节点上运行着[zookeeper](http://zookeeper.apache.org/)，然后运行zookeeper client：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/zookeeper-client.md %}

在zookeeper client终端中，运行以下命令查看当前leader：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/see-leader.md %}

该命令的输出结果应当包含了当前的leader，由于新的leader要通过选举确定，可能要等待一会。你可以在[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1)中通过该leader的名称查询其公共IP。

在浏览器中输入`http://{NEW_LEADER_MASTER_IP}:19999`地址访问Tachyon web UI，在选项卡中点击`Browse File System`，可以看到所有文件依然存在。

在集群中的某个节点上，可以通过ssh免密码登陆到集群中的其他节点：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/ssh-other-node.md %}

# 销毁集群

在`deploy/vagrant`目录下运行：

{% include Running-Tachyon-Fault-Tolerant-on-EC2/destroy.md %}

从而销毁之前创建的集群。一次只能创建一个集群。当该命令成功执行后，EC2实例将终止运行。
