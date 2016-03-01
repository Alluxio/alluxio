---
layout: global
title: 在Swift上配置Alluxio
nickname: Alluxio使用Swift
group: Under Store
priority: 1
---

该指南介绍如何配置Alluxio以使用[Swift](http://docs.openstack.org/developer/swift/)作为底层文件系统。

# 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有配置文件，可以由template文件创建配置文件：

{% include Common-Commands/copy-alluxio-env.md %}

# 配置Alluxio

若要在Alluxio中使用Swift作为底层文件系统，一定要修改`conf/alluxio-env.sh`配置文件。首先要指定Swift的地址，在`conf/alluxio-env.sh`中添加：

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

其中`<swift-container>`是一个已有的Swift容器。

以下的配置项也应包含在`conf/alluxio-env.sh`文件中：

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}
  	
`<swift-use-public>`的值为`true`或`false`。
`<swift-auth-model>`的值为`keystone`、`tempauth`或`swiftauth`。

在成功授权情况下，Keystone会返回两个访问URL：公共的和私有的。如果Alluxio是在公司网络中使用，并且Swift也在同一个网络中，建议设置`<swift-use-public>`的值为`false`。


## 访问IBM SoftLayer对象存储

使用Swift的配置，也能够将IBM SoftLayer对象存储作为Alluxio底层文件系统，SoftLayer需要将`<swift-auth-model>`设置为`swiftauth`。
 
# 在本地Swift上运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的Swift容器，其中应包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}

# 对IBM SoftLayer进行功能测试

在`tests/pom.xml`配置你的Swift或者SoftLayer账户，其中`authMethodKey`的值应为`keystone`、`tempauth`或`swiftauth`，要进行功能测试，运行：

{% include Configuring-Alluxio-with-Swift/functional-tests.md %}

若测试失败，日志记录在`tests/target/logs`下。可以通过以下命令抓取堆状态备份：

{% include Configuring-Alluxio-with-Swift/heap-dump.md %}
