---
layout: global
title: 在Ceph上配置Alluxio
nickname: Alluxio使用Ceph
group: Under Store
priority: 1
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用Ceph作为底层文件系统。Alluxio使用[Rados Gateway](http://docs.ceph.com/docs/master/radosgw/)
支持两种不同的客户端API连接[Ceph Object Storage](http://ceph.com/ceph-storage/object-storage/)：
- [S3A](http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) (推荐使用)
- [Swift](http://docs.openstack.org/developer/swift/)

## 初始步骤

首先，在你的机器上必须安装Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Alluxio

为了配置Alluxio以使用底层文件系统，需要修改`alluxio-site.properties`文件。如果该文件不存在，根据模板创建配置文件。

```bash
cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

### 方法1: S3A接口

向`conf/alluxio-env.sh`文件添加以下代码：

{% include Configuring-Alluxio-with-Ceph/underfs-s3a.md %}

如果使用Ceph的版本，比如hammer(或者更老的版本)，指定`alluxio.underfs.s3a.signer.algorithm=S3SignerType`来使用v2版本的S3签名。
为了使用v1版本的GET Bucket(列出对象)，指定`alluxio.underfs.s3a.list.objects.v1=true`。

### 方法2: Swift接口

向`conf/alluxio-env.sh`文件添加以下代码：

{% include Configuring-Alluxio-with-Swift/underfs-address.md %}

其中，`<swift-container>`是一个已经存在的Swift容器。

以下的配置需要配置在`conf/alluxio-site.properties`中

{% include Configuring-Alluxio-with-Swift/several-configurations.md %}

`<swift-use-public>`可能的值为`true`，`false`。如果使用本地的Ceph RGW认证，指定`<swift-auth-model>`为`swiftauth`。指定
`<swift-auth-url>`为`http://<rgw-hostname>:8090/auth/1.0`

## 使用Ceph本地运行Alluxio

完成所有的配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的bucket/container目录，确认其中包含了由Alluxio创建的文件和目录。

如果使用S3A连接器，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-S3/s3-file.md %}

如果使用Swift连接器，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-Swift/swift-files.md %}

## 访问控制

如果Alluxio安全认证被启用，Alluxio将会遵循底层Ceph对象存储的访问权限控制，根据使用的接口，参考[S3A Access Control](Configuring-Alluxio-with-S3.html#s3-access-control)
或者[Swift Access Control](Configuring-Alluxio-with-Swift.html#swift-access-control)获得更多信息。

