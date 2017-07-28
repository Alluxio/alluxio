---
layout: global
title: 在GCS上配置Alluxio
nickname: Alluxio使用GCS
group: Under Store
priority: 0
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Google Cloud Storage](https://cloud.google.com/storage/)作为底层文件系统。

## 初始步骤

首先，在你的机器上必须安装Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

接着，如果还没有配置，请用‘bootstrapConf'命令创建自己的配置。
例如，如果你在本地机器运行Alluxio，就需要将`ALLUXIO_MASTER_HOSTNAME`设置为`localhost`

{% include Configuring-Alluxio-with-GCS/bootstrapConf.md %}

或者，您也可以从模板创建配置文件并手动设置内容。

{% include Common-Commands/copy-alluxio-env.md %}

而且，为了在Alluxio上使用GCS, 需要创建一个bucket（或者使用一个已有的bucket)。你应该注意你在这个bucket里准备使用的目录，你可以自己在这个bucket里面创建一个新目录，或者使用一个已有的目录。在这个指南中，我们将GCS bucket取名为`GCS_BUCKET`，bucket中的目录取名为`GCS_DIRECTORY`。

如果你刚接触GCS，请先阅读GCS[文档](https://cloud.google.com/storage/docs/overview)。

## 配置Alluxio

为了配置Alluxio以使用GCS作为其底层文件系统，需要修改`conf/alluxio-env.sh`文件。第一个需要修改的地方就是指定一个已有的bucket以及bucket中的目录作为底层文件系统。你可以通过向`conf/alluxio-env.sh`文件添加以下代码来指定：

{% include Configuring-Alluxio-with-GCS/underfs-address.md %}

接下来，你需要指定Google证书来接入GCS。在`conf/alluxio-env.sh`文件的
`ALLUXIO_JAVA_OPTS`部分添加:

{% include Configuring-Alluxio-with-GCS/google.md %}

在这里 `<GCS_ACCESS_KEY_ID>` 和 `<GCS_SECRET_ACCESS_KEY>`应该用你的
[GCS可互操作存储的access keys](https://console.cloud.google.com/storage/settings)来替换,或者用其他包含你的证书的环境变量来替换。

注意: GCS的可互操作性默认是未启用的。请在[GCS 设置](https://console.cloud.google.com/storage/settings) 中点击可互操作按键来启用可互操作性。然后点击`创建一个新的key`来获得the Access Key和Secret pair.

执行完以上步骤后，Alluxio应该已经配置GCS作为其底层文件系统，然后你可以尝试[使用GCS本地运行Alluxio](#running-alluxio-locally-with-gcs).

## 配置应用依赖

当使用Alluxio构建你的应用时，你的应用需要包含一个client模块，如果要使用[Alluxio file system interface](File-System-API.html)，那么需要配置`alluxio-core-client-fs`模块，如果需要使用[Hadoop file system interface](https://wiki.apache.org/hadoop/HCFS)，则需要使用`alluxio-core-client-hdfs`模块。
举例来说，如果你正在使用 [maven](https://maven.apache.org/)，你可以通过添加以下代码来添加你的应用的依赖：

{% include Configuring-Alluxio-with-GCS/dependency.md %}

## 使用GCS本地运行Alluxio

完成所有的配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的GCS目录`GCS_BUCKET/GCS_DIRECTORY`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-GCS/gcs-file.md %}

运行以下命令停止Alluxio:

{% include Common-Commands/stop-alluxio.md %}

## GCS访问控制

如果Alluxio安全认证被启用，Alluxio将会遵循底层对象存储的访问权限控制。

在Alluxio配置中指定的GCS证书代表一个GCS用户，GCS服务终端会在用户试图访问bucket和对象时检查其权限，如果该GCS用户没有访问该bucket的权限，将会抛出一个权限错误。Alluxio在第一次将元数据从底层GCS加载到Alluxio命名空间时，便会同时将其bucket的ACL也加载到Alluxio权限管理元数据中。

### GCS用户到Alluxio文件所有者的映射关系

默认情况下， Alluxio会尝试从证书中解析其GCS用户id。另外，可以配置`alluxio.underfs.gcs.owner.id.to.username.mapping`从而指定某个GCS用户id到Alluxio用户名的映射关系，其配置形式为"id1=user1;id2=user2"。谷歌云存储ID可以在[该控制台地址](https://console.cloud.google.com/storage/settings)找到，请使用“Owners”这一项。

### GCS ACL到Alluxio权限的映射关系

Alluxio通过检查GCS bucket的读写ACL来确定Alluxio文件的权限。举例来说，如果某个GCS用户对一个底层bucket具有只读权限，在Alluxio中该挂载的目录以及文件的权限模式将为0500,如果该GCS用户具有所有权限，那么权限模式将为0700。

### 挂载点共享

如果你想在Alluxio命名空间中与其他用户共享GCS挂载点，可以启用`alluxio.underfs.object.store.mount.shared.publicly`。

### 权限更改

注意，对Alluxio目录或者文件运行chown/chgrp/chmod等命令不会对底层GCS bucket或者对象的权限做出更改。
