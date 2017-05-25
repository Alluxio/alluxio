---
layout: global
title: 在Swift上配置Alluxio
nickname: Alluxio使用Swift
group: Under Store
priority: 1
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio从而使其底层存储系统支持[Swift API](http://docs.openstack.org/developer/swift/)。

## 初始安装步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有配置文件，可以使用`bootstrapConf`命令来创建自己的配置文件。
举个例子，假如你正在本地运行Alluxio，那么就应该把`ALLUXIO_MASTER_HOSTNAME`设置为`localhost`。

{% include Configuring-Alluxio-with-Swift/bootstrapConf.md %}

另外你也可以由template文件创建配置文件，并且手动修改它的内容：

{% include Common-Commands/copy-alluxio-env.md %}

## 配置Alluxio

如果你需要将Swift用作Alluxio的底层存储系统，你需要修改`conf/alluxio-site.properties`，在其中添加：

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

其中`<swift-container>`是一个已有的Swift容器。

以下的配置项也应包含在`conf/alluxio-site.properties`文件中：

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}

`<swift-use-public>`的值为`true`或`false`。
`<swift-auth-model>`的值为`keystonev3`,`keystone`、`tempauth`或`swiftauth`。
当采用任意一个`keystone`认证时，下面的参数可以有选择地设置：

{% include Configuring-Alluxio-with-Swift/keystone-region-configuration.md %}

另外，这些配置设置能够在`conf/alluxio-env.sh`文件中设置。更多设置配置参数的细节可以在
[Configuration Settings](Configuration-Settings.html#environment-variables)中找到。

认证成功以后，Keystone会返回两个访问URL: 公开的 和 私有的。如果Alluxio用在公司内部网，并且Swift位于同样的网络中，那么建议设置`<swift-use-public>`的值为`false`。


## Swift对象存储选项

使用Swift模块使得Alluxio能使用[Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/)以及[IBM SoftLayer](http://www.softlayer.com/object-storage)对象存储作为底层存储。若要使用Ceph，必须部署[Rados Gateway](http://docs.ceph.com/docs/master/radosgw/)模块。

## 在本地Swift上运行Alluxio

所有配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的Swift容器，其中应该包含了由Alluxio创建的文件和目录。在这个测试中，你应该会看到创建的文件名像下面这样：

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}

## 运行功能测试


在`tests/pom.xml`配置`swiftTest`下的Swift账户凭据，其中`authMethodKey`的值应为`keystone`、`tempauth`或`swiftauth`，要进行功能测试，运行：

{% include Configuring-Alluxio-with-Swift/functional-tests.md %}

若测试失败，日志记录在`tests/target/logs`下。可以通过以下命令抓取堆状态备份：

{% include Configuring-Alluxio-with-Swift/heap-dump.md %}

## Swift访问控制

如果Alluxio的安全模式被启用，那么Alluxio会强制让访问控制继承自底层的对象存储。

在Alluxio中指定的Swift凭证(`fs.swift.user`, `fs.swift.tenant` and `fs.swift.password`) 代表了一个Swift用户。Swift服务的后端会检查访问容器的用户许可。
如果给定的Swift用户没有访问特定容器的正确的访问许可，将会抛出一个许可被拒绝的错误。
一旦Alluxio安全模式被启用，当元数据第一次加载到Alluxio命名空间时，Alluxio将会加载容器ACL到Alluxio许可中。

#### 挂载点分享
如果你想分享一个Swift挂载点给其他在Alluxio命名空间里的用户，你可以启用`alluxio.underfs.object.store.mount.shared.publicly`。

#### 改变许可
除此之外，对Alluxio目录和文件的chown/chgrp/chmod操作既不会传播到底层Swift容器，也不会传播到Swift对象。
