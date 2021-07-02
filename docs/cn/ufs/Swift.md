---
layout: global
title: Alluxio集成Swift作为底层存储
nickname: Alluxio集成Swift作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio从而使其底层存储系统支持[Swift API](http://docs.openstack.org/developer/swift/)。

## 初始安装步骤
  
首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Alluxio

要使用底层存储系统，你需要编辑`conf/alluxio-site.properties`来配置Alluxio。如果该文件不存在，那就从模板创建一个配置文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

修改`conf/alluxio-site.properties`配置文件的内容包括：
  
```properties
alluxio.master.mount.table.root.ufs=swift://<container>/<folder>
fs.swift.user=<swift-user>
fs.swift.tenant=<swift-tenant>
fs.swift.password=<swift-user-password>
fs.swift.auth.url=<swift-auth-url>
fs.swift.auth.method=<swift-auth-model>
```

使用现有的Swift容器地址替换`<container>/<folder>`。`<swift-use-public>`的值为`true`或`false`。`<swift-auth-model>`的值为`keystonev3`、`keystone`、`tempauth`或`swiftauth`。 

当采用任意一个keystone认证时，下面的参数可以有选择地设置： 

```properties
fs.swift.region=<swift-preferred-region>
```

认证成功以后，Keystone会返回两个访问URL: 公开的 和 私有的。如果Alluxio用在公司内部网，并且Swift位于同样的网络中，那么建议设置`<swift-use-public>`的值为`false`。

## Swift对象存储选项

使用Swift模块使得Alluxio能使用[Ceph Object Storage](https://ceph.com/ceph-storage/object-storage/)以及[IBM SoftLayer](http://www.softlayer.com/object-storage)对象存储作为底层存储。若要使用Ceph，必须部署[Rados Gateway](http://docs.ceph.com/docs/master/radosgw/)模块。

## 使用Swift本地运行Alluxio

完成配置后，你可以启动一个Alluxio集群：

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

```console
$ ./bin/alluxio runTests
```

运行成功后，访问你的Swift容器，其中应该包含了由Alluxio创建的文件和目录。在这个测试中，你应该会看到创建的文件名像下面这样：

```
<container>/<folder>/default_tests_files/Basic_CACHE_THROUGH
```

运行以下命令停止Alluxio：

```console
$ ./bin/alluxio-stop.sh local
```

## 运行功能测试  

```console
$ mvn test -DtestSwiftContainerKey=swift://<container>
```
## 运行功能测试

以下命令可用于测试给定的Swift凭证是否有效。 开发人员还可以使用它对Swift endpoint运行功能测试，以验证Alluxio和Swift之间的合约。

```console
$ ./bin/alluxio runUfsTests --path swift://<bucket> \
  -Dfs.swift.user=<SWIFT_USER> \
  -Dfs.swift.tenant=<SWIFT_TENANT> \
  -Dfs.swift.password=<SWIFT_PASSWORD> \
  -Dfs.swift.auth.url=<AUTH_URL> \
  -Dfs.swift.auth.method=<AUTH_METHOD> 
```

## Swift访问控制

如果Alluxio的安全模式被启用，那么Alluxio会强制让访问控制继承自底层的对象存储。

在Alluxio中指定的Swift凭证(`fs.swift.user`, `fs.swift.tenant` and `fs.swift.password`) 代表了一个Swift用户。Swift服务的后端会检查访问容器的用户许可。
如果给定的Swift用户没有访问特定容器的正确的访问许可，将会抛出一个许可被拒绝的错误。
一旦Alluxio安全模式被启用，当元数据第一次加载到Alluxio命名空间时，Alluxio将会加载容器ACL到Alluxio许可中。

#### 挂载点分享
如果你想分享一个Swift挂载点给其他在Alluxio命名空间里的用户，你可以启用`alluxio.underfs.object.store.mount.shared.publicly`。

#### 改变许可
除此之外，对Alluxio目录和文件的chown/chgrp/chmod操作既不会传播到底层Swift容器，也不会传播到Swift对象。
