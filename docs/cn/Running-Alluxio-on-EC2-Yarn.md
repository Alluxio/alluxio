---
layout: global
title: 在EC2上使用YARN运行Alluxio
nickname: 在EC2上使用YARN运行Alluxio
group: Deploying Alluxio
priority: 5
---

* 内容列表
{:toc}

Alluxio可以由Apache YARN启动并管理。该指南介绍如何使用Alluxio自带的
[Vagrant脚本](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant)在EC2的机器上用YARN启
动Alluxio。

## 前期准备

**安装Vagrant和AWS插件**

下载[Vagrant](https://www.vagrantup.com/downloads.html)

安装AWS Vagrant插件:

{% include Running-Alluxio-on-EC2-Yarn/install-vagrant-aws.md %}

**安装Alluxio**

下载Alluxio到本地，并解压：

{% include Running-Alluxio-on-EC2-Yarn/download-Alluxio-unzip.md %}

**安装python库依赖**

安装[python>=2.7](https://www.python.org/)，注意不是python3。

进入Alluxio主目录下的`deploy/vagrant` 目录下，运行：

{% include Running-Alluxio-on-EC2-Yarn/install-python.md %}

你可以选择手动安装[pip](https://pip.pypa.io/en/latest/installing/)，进入deploy/vagrant目录，运行：

{% include Running-Alluxio-on-EC2-Yarn/install-pip.md %}

## 启动集群

要在EC2上运行Alluxio集群，首先在[Amazon Web Services site](http://aws.amazon.com/)注册一个Amazon EC2帐号。

接着创建[access keys](https://aws.amazon.com/developers/access-keys/)并且设置`AWS_ACCESS_KEY_ID`和`AWS_SECRET_ACCESS_KEY` shell环境变量:

{% include Running-Alluxio-on-EC2-Yarn/access-key.md %}

在你想部署的区域生成EC2
[Key Pairs](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)(默认为**us-east-1**)。确保将私钥文件的权限设置成只有你可读：

{% include Running-Alluxio-on-EC2-Yarn/generate-key-pair.md %}

将`deploy/vagrant/conf/ec2.yml.template`文件复制一份，命名为`deploy/vagrant/conf/ec2.yml`，并在其中将`Keypair`设置为你的keypair名，`Key_Path`设置成pem key路径。

Vagrant脚本默认会在[区域(**us-east-1**)和可用区域(**us-east-1b**)](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html)中创建一个名为*alluxio-vagrant-test*的[安全组](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)。
该安全组会在区域中自动建立，而且所有入站及出站的网络流量都将打开。你可以在`ec2.yml`配置文件中更改安全组、区域以及可用区域。

最后，在`deploy/vagrant/conf/ufs.yml`文件中设置"Type"的值为`hadoop2`。

现在你可以运行`deploy/vagrant`下的脚本在us-east-1b区域启动Alluxio集群，该集群以Hadoop2.4.1作为底层文件系统：

{% include Running-Alluxio-on-EC2-Yarn/launch-Alluxio.md %}

## 访问集群

**通过Web UI访问**

`./create <number of machines> aws`命令运行成功后，shell输出的末尾会有如下两行(shell中为绿色)：

{% include Running-Alluxio-on-EC2-Yarn/shell-end.md %}

Alluxio Web UI的默认端口为**19999**。

Hadoop Web UI的默认端口为**50070**。

在浏览器中输入`http://{MASTER_IP}:{PORT}`地址访问Web UI。

也能通过[AWS web console](https://console.aws.amazon.com/console/home?region=us-east-1)监视其状态。

**通过ssh访问**

节点的名称依次被设置成`AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2`等等。

通过ssh登陆一个节点，运行：

{% include Running-Alluxio-on-EC2-Yarn/ssh-node.md %}

例如，ssh `AlluxioMaster`节点：

{% include Running-Alluxio-on-EC2-Yarn/ssh-master.md %}

所有的软件都安装在根目录下，例如Alluxio安装在`/alluxio`，Hadoop安装在`/hadoop`。

## 配置集成YARN的Alluxio

在我们的EC2机器上，YARN作为Hadoop2.4.1的一部分安装。注意，vagrant脚本构建的Alluxio二进制版本并不包含YARN
的整合。需要先停止默认的Alluxio服务，重新编译Alluxio，编译时指定"yarn"配置文件(以便Alluxio包含YARN client
和ApplicationMaster)。

{% include Running-Alluxio-on-EC2-Yarn/stop-install-yarn.md %}

添加`-DskipTests -Dfindbugs.skip -Dmaven.javadoc.skip -Dcheckstyle.skip`不是严格必须的，但是添加后可以使构建过程快很多。

定制Alluxio master和worker的特定属性(例如，每个worker建立分层存储)，参考
[配置设置](Configuration-Settings.html)获取更多信息。为了确保你的配置可以被ApplicationMaster和
Alluxio master/worker读取,将其设置在`alluxio-site.properties`中。

## 启动Alluxio

如果Yarn不是存放在HADOOP_HOME目录下，则需要将Yarn的基本路径保存到YARN_HOME环境变量。

使用`integration/yarn/bin/alluxio-yarn.sh`脚本启动Alluxio。该脚本有3个参数：
1. 需要启动的Alluxio worker的总数。(必填项)
2. 分布存储Alluxio ApplicationMaster可执行文件的HDFS路径。(必填项)
3. 运行Alluxio Master的节点的YARN的名称。（选填项，默认为`ALLUXIO_MASTER_HOSTNAME`）


举例而言，启动3个worker节点的Alluxio集群，HDFS临时目录是`hdfs://AlluxioMaster:9000/tmp/`,主机的名字是`AlluxioMaster`

你也可以在Yarn之外启动Alluxio Master节点，在这种情况下，上述启动过程将会自动检测所提供地址的主机上的Master，并且跳过该新实例的初始化。这非常有用，特别是当你想在某台特定的主机上运行Master，而该主机不属于Yarn集群，例如一个AWS EMR Master实例。


{% include Running-Alluxio-on-EC2-Yarn/three-arguments.md %}

该脚本会在YARN上启动Application Master，后面会继续为Alluxio master和workers申请运行的容器。脚本一直运行并报告ApplicationMaster的状态。也可以在浏览器中查看`http://AlluxioMaster:8088`访问Web UI观察Alluxio作业的状态和应用ID。

以上脚本会产生如下的输出：

{% include Running-Alluxio-on-EC2-Yarn/script-output.md %}

从输出中，我们得知运行Alluxio的应用ID是**`application_1445469376652_0002`**。应用ID可以用来杀死该应用。

## 测试Alluxio

知道Alluxio master容器的IP后，可以修改`conf/alluxio-env.sh`在每台EC2机器上建立`ALLUXIO_MASTER_HOSTNAME`环境变量：

{% include Running-Alluxio-on-EC2-Yarn/environment-variable.md %}

可以对Alluxio运行测试检测其健康状态：

{% include Running-Alluxio-on-EC2-Yarn/runTests.md %}

在所有测试完成后，再次访问Alluxio的web UI `http://{MASTER_IP}:19999`，在导航栏中点击`Browse`，可以看到测试过程中写入到Alluxio的文件。


# 停止Alluxio

使用如下YARN命令可以停止Alluxio，其中应用ID可以从YARN web UI或`alluxio-yarn.sh`的输出中获取（上面已经提及）。举例而言，如果应用ID是`application_1445469376652_0002`，可以使用如下语句杀死应用：

{% include Running-Alluxio-on-EC2-Yarn/kill-application.md %}

## 撤销集群

在启动EC2机器的本地机器`deploy/vagrant`目录下运行：

{% include Running-Alluxio-on-EC2-Yarn/destroy.md %}

撤销之前创建的集群。一次只能创建一个集群。当该命令成功执行后，EC2实例将终止运行。

## 故障排除

1 如果使用maven编译集成YARN的Alluxio，编译时报错显示如下信息：

{% include Running-Alluxio-on-EC2-Yarn/compile-error.md %}

请确保使用正确的Hadoop版本：
{% include Running-Alluxio-on-EC2-Yarn/Hadoop-version.md %}
