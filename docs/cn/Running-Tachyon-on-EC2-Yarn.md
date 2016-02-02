---
layout: global
title: 在EC2上使用YARN运行Tachyon
nickname: 在EC2上使用YARN运行Tachyon
group: User Guide
priority: 5
---

Tachyon可以由Apache YARN启动并管理。该向导介绍如何使用Tachyon自带的
[Vagrant脚本](https://github.com/amplab/tachyon/tree/master/deploy/vagrant)在EC2的机器上用YARN启
动Tachyon。

# 前期准备

**安装Vagrant和AWS插件**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

安装AWS Vagrant插件:

{% include Running-Tachyon-on-EC2-Yarn/install-vagrant-aws.md %}

**安装Tachyon**

下载Tachyon到本地，并解压：

{% include Running-Tachyon-on-EC2-Yarn/download-Tachyon-unzip.md %}

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3。

进入Tachyon主目录下的`deploy/vagrant` 目录下，运行：

{% include Running-Tachyon-on-EC2-Yarn/install-python.md %}

你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，进入deploy/vagrant目录，运行：

{% include Running-Tachyon-on-EC2-Yarn/install-pip.md %}

# 启动集群

要在EC2上运行Tachyon集群，首先在[Amazon Web Services site](http://aws.amazon.com/)注册一个Amazon EC2帐号。

接着创建[access keys](https://aws.amazon.com/developers/access-keys/)并且设置`AWS_ACCESS_KEY_ID`和`AWS_SECRET_ACCESS_KEY`shell环境变量:

{% include Running-Tachyon-on-EC2-Yarn/access-key.md %}

在你想部署的区域生成EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)(默认为**us-east-1**)。确保将私钥文件的权限设置成只有你可读：

{% include Running-Tachyon-on-EC2-Yarn/generate-key-pair.md %}

在`deploy/vagrant/conf/ec2.yml`配置文件中，将`Keypair`设置为你的keypair名，`Key_Path`设置成pem key路径。

Vagrant脚本默认会在[区域(**us-east-1**)和可用区域(**us-east-1a**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html)中创建一个名为*tachyon-vagrant-test*的[安全组](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)。
该安全组会在区域中自动建立，而且所有入站及出站的网络流量都将打开。你可以在`ec2.yml`配置文件中设置*security group*、*region*以及*availability zone*的值。

现在可以启动Tachyon集群，该集群在in us-east-1a中以Hadoop2.4.1为底层文件系统，运行`deploy/vagrant`下的脚本：

{% include Running-Tachyon-on-EC2-Yarn/launch-Tachyon.md %}

# 访问集群

**通过Web UI访问**

`./create <number of machines> aws`命令运行成功后，shell输出的末尾会有如下两行(shell中为绿色)：

{% include Running-Tachyon-on-EC2-Yarn/shell-end.md %}

Tachyon Web UI的默认端口为**19999**。

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

也能通过[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1)监视其状态。

**通过ssh访问**

节点的名称依次被设置成`TachyonMaster`, `TachyonWorker1`, `TachyonWorker2`等等。

通过ssh登陆一个节点，运行：

{% include Running-Tachyon-on-EC2-Yarn/ssh-node.md %}

例如，ssh `TachyonMaster`节点：

{% include Running-Tachyon-on-EC2-Yarn/ssh-master.md %}

所有的软件都安装在根目录下，例如Tachyon安装在`/tachyon`，Hadoop安装在`/hadoop`。

# 配置集成YARN的Tachyon

在我们的EC2机器上，YARN作为Hadoop2.4.1的一部分安装。注意，vagrant脚本构建的Tachyon二进制版本并不包含YARN
的整合。需要先停止默认的Tachyon服务，重新编译Tachyon，编译时指定"yarn"配置文件(以便Tachyon包含YARN client
和ApplicationMaster)。

{% include Running-Tachyon-on-EC2-Yarn/stop-install-yarn.md %}

添加`-DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip`不是严格必须的，但是添加后可以
使构建过程快很多。

定制Tachyon master和worker的特定属性(例如，每个worker建立分层存储)，参考
[配置设置](Configuration-Settings.html)获取更多信息。为了确保你的配置可以被ApplicationMaster和
Tachyon master/worker读取,将`${TACHYON_HOME}/conf`下的`tachyon-site.properties`放在每一台EC2机器上。

# 启动Tachyon

使用`integration/bin/tachyon-yarn.sh`脚本启动Tachyon。该脚本需要3个参数：
1. 每台机器上指向`${TACHYON_HOME}`的路径。以便YARN NodeManager可以访问Tachyon脚本和可执行文件启动master
和worker。在我们创建的EC2上，该路径为`/tachyon`。
2. 需要启动的Tachyon worker的总数。
3. 分布存储Tachyon ApplicationMaster可执行文件的HDFS路径。

举例而言，启动3个worker节点的Tachyon集群，HDFS临时目录是`hdfs://TachyonMaster:9000/tmp/`并且每个YARN容
器可以在`/tachyon`目录下访问Tachyon:

{% include Running-Tachyon-on-EC2-Yarn/three-arguments.md %}

脚本先上传YARN client和ApplicationMaster的可执行文件到指定的HDFS路径，再通知YARN运行client二进制jar。脚
本一直运行并报告ApplicationMaster的状态。也可以在浏览器中查看`http://TachyonMaster:8088`访问Web UI观察
Tachyon作业的状态和应用ID。

以上脚本会产生如下的输出：

{% include Running-Tachyon-on-EC2-Yarn/script-output.md %}

从输出中，我们得知运行Tachyon的应用ID是**`application_1445469376652_0002`**。应用ID可以用来杀死该应用。

提示：当前的Tachyon YARN框架不保证在TachyonMaster机器上启动Tachyon。使用YARN web UI读取YARN应用的日志。
应用的日志会记录哪台机器启动了Tachyon master容器，如下所示：

{% include Running-Tachyon-on-EC2-Yarn/log-Tachyon-master.md %}

# 测试Tachyon

知道Tachyon master容器的IP后，可以修改`conf/tachyon-env.sh`在每台EC2机器上建立`TACHYON_MASTER_ADDRESS`环境变量：

{% include Running-Tachyon-on-EC2-Yarn/environment-variable.md %}

可以对Tachyon运行测试检测其健康状态：

{% include Running-Tachyon-on-EC2-Yarn/runTests.md %}

在所有测试完成后，再次访问Tachyon的web UI `http://{MASTER_IP}:19999`，在导航栏中点击`Browse File System`，可以看到测试过程中写入到Tachyon的文件。


# 停止Tachyon

使用如下YARN命令可以停止Tachyon，其中应用ID可以从YARN web UI或`tachyon-yarn.sh`的输出中获取（上面已经提及）。举例而言，如果应用ID是`application_1445469376652_0002`，可以使用如下语句杀死应用：

{% include Running-Tachyon-on-EC2-Yarn/kill-application.md %}

# 销毁集群

在启动EC2机器的本地机器`deploy/vagrant`目录下运行：

{% include Running-Tachyon-on-EC2-Yarn/destroy.md %}

销毁之前创建的集群。一次只能创建一个集群。当该命令成功执行后，EC2实例将终止运行。

# 故障排除

1 如果使用maven编译集成YARN的Tachyon，编译时报错显示如下信息：

{% include Running-Tachyon-on-EC2-Yarn/compile-error.md %}

请确保使用正确的Hadoop版本：
{% include Running-Tachyon-on-EC2-Yarn/Hadoop-version.md %}