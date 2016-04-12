---
layout: global
title: 在GCE上运行Alluxio
nickname: 在GCE上运行Alluxio
group: User Guide
priority: 4
---

使用Alluxio自带的
[Vagrant scripts](https://github.com/alluxio/alluxio/tree/master/deploy/vagrant)脚本可以将Alluxio部署在Google计算引擎（GCE）上。该脚本允许你创建，配置以及撤销集群。

# 前期准备

**安装Vagrant和Google插件**

下载 [Vagrant](https://www.vagrantup.com/downloads.html)

安装Google Vagrant 插件:

{% include Running-Alluxio-on-GCE/install-google-vagrant-plugin.md %}

**安装 Alluxio**

下载Alluxio到本地，并解压:

{% include Common-Commands/download-alluxio.md %}

**安装python依赖库**

安装 [python>=2.7](https://www.python.org/), 注意不是 python3.

进入 `deploy/vagrant` 目录下，运行:

{% include Running-Alluxio-on-GCE/install-vagrant.md %}

另外，你可以搜互动选择安装 [pip](https://pip.pypa.io/en/latest/installing/), 之后进入 `deploy/vagrant` 目录，运行:

{% include Running-Alluxio-on-GCE/install-pip.md %}

# 启动集群

要在GCE上运行Alluxio集群, 首先在 [Google Cloud](cloud.google.com) 上有一个计费账号，项目，服务账户和JSON服务账号的密钥。

如果你未注册过Google Cloud, 你需要在 [free trial signup page](https://console.cloud.google.com/billing/freetrial)上创建一个计费账号和项目。 同样地，如果你不熟悉Google计算引擎，你可以先在 [documentation](http://cloud.google.com/compute/docs)上回顾一下关于这部分的内容。

接着, 对于一个 Google Cloud 新用户来说需要在 [Console](console.google.com) 上的 [Permissions](http://console.cloud.google.com/permissions) 页面内 [Service Accounts](http://console.cloud.google.com/permissions) 标签上选择或者创建一个服务账号。
如果你创建好了一个新的服务账号, 请在账号创建的对话框中核对 "Furnish a new private key."信息 。下载JSON密钥并将它存储在一个安全的位置。
如果你是使用了一个已有的服务账号, 你需要使用这个账号已有的JSON密钥或者重新下载一个新的。 你可以在 [Service Accounts](http://console.cloud.google.com/permissions) 标签中,在服务账号的列表右侧的菜单栏中的三个省略号下找到并学则“创建密钥”的按钮，为已有的服务账号下载一个新的JSON密钥，并将它存储在一个安全的位置。

为 [gcloud sdk](http://console.cloud.google.com) 配置ssh密钥:

{% include Running-Alluxio-on-GCE/config-ssh.md %}

复制 `deploy/vagrant/conf/gce.yml.template` to `deploy/vagrant/conf/gce.yml` :

{% include Running-Alluxio-on-GCE/copy-gce.md %}

在 `deploy/vagrant/conf/gce.yml`配置文件中,设置你的 项目 id,服务账号, JSON密钥的位置和已经创建好的ssh用户名.

目前GCE默认使用 Hadoop2 作为它的底层文件系统, 因为目前Alluxio并不支持GCS作为底层文件系统。

现在你可以启动Alluxio集群了，通过在 `deploy/vagrant`目录下运行:

{% include Running-Alluxio-on-GCE/launch-cluster.md %}

集群中的每个节点运行一个Alluxio worker, `AlluxioMaster` 节点上运行Alluxio master。

# 访问集群

**通过Web UI访问**

命令 `./create <number of machines> google` 运行成功后, 在shell中会输出类似下面的两条语句:

{% include Running-Alluxio-on-GCE/shell-output.md %}

Alluxio Web UI的默认端口为 **19999**.

在访问Web UI之前, 需要配置防火墙以允许19999端口上的tcp传输。
可以通过在 [Console](console.cloud.google.com) UI 上完成或者使用类似如下的gcloud命令，假设网络名是 'default'.

{% include Running-Alluxio-on-GCE/add-firewall-rule.md %}

在浏览器中输入 `http://{MASTER_IP}:{PORT}` 地址访问Web UI。

也可以通过
[Google Cloud console](https://console.cloud.google.com)监视其状态。

这里是一些当你检查控制台时，可能会遇到的问题:
 - 当集群创建失败，请检查 status/logs实例日志。
 - 集群奔溃后，确保 GCE 实例被终止。
 - 当不再使用集群时，确保 GCE 实例不会再占用额外的内存。

**通过ssh访问**

节点的名称依次被设置成 `AlluxioMaster`, `AlluxioWorker1`, `AlluxioWorker2`等等。

通过ssh登陆一个节点，运行：

{% include Running-Alluxio-on-GCE/ssh.md %}

例如，通过以下命令可以登陆 `AlluxioMaster`节点:

{% include Running-Alluxio-on-GCE/ssh-AlluxioMaster.md %}

所有的软件都安装在根目录下，例如Alluxio安装在 `/alluxio`。

在 `AlluxioMaster` 节点上，可以对Alluxio运行测试检测其健康状态:

{% include Running-Alluxio-on-GCE/runTests.md %}

在所有测试完成后，再次访问Alluxio的web UI `http://{MASTER_IP}:19999`，在导航栏中点击 `Browse
File System` 你应该能看到测试过程中写入到Alluxio的文件。

在集群中的某个节点上，可以通过ssh免密码登陆到集群中的其他节点：

{% include Running-Alluxio-on-GCE/ssh-other-node.md %}

# 撤销集群

在 `deploy/vagrant` 目录下运行：

{% include Running-Alluxio-on-GCE/destroy.md %}

从而撤销之前创建的集群。一次只能创建一个集群。当该命令成功执行后，GCE 实例将终止运行。
