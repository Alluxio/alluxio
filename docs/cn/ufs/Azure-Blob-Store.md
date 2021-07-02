---
layout: global
title: Alluxio集成Azure Blob Store作为底层存储
nickname: Alluxio集成Azure Blob Store作为底层存储
group: Storage Integrations
priority: 2
---

* 内容列表
{:toc}

该指南介绍了如何配置Alluxio以使用[Azure Blob Store](https://azure.microsoft.com/en-in/services/storage/blobs/)作为底层存储系统。

## 初始步骤

为了在多台机器上运行一个Alluxio集群，你必须在每台机器上部署Alluxio的二进制文件。
你可以[从Alluxio源码编译二进制文件](Building-Alluxio-From-Source.html)，或者[直接下载已经编译好的Alluxio二进制文件](Running-Alluxio-Locally.html)。

此外，为了在Alluxio上使用Azure Blob Store，你需要在Azure storage帐户上创建一个新的container或者使用一个已有的container。还要提供准备在这个container里使用的目录，你可以在这个container里面创建一个目录，或者使用一个已有的目录。在这篇文章中，我们将Azure storage帐户名称为`<AZURE_ACCOUNT>`，将帐户里的container称为`<AZURE_CONTAINER>`并将该container里面的目录称为`<AZURE_DIRECTORY>`。点击[这里](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account)查看更多关于Azure storage帐户的信息。


## 配置Alluxio

### 根挂载

要使用Azure blob store作为Alluxio根挂载点的UFS，您需要通过修改`conf/alluxio-site.properties`配置Alluxio使用底层存储系统。如果这个文件不存在，重命名template文件。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

首先修改`conf / alluxio-site.properties`来指定underfs address：

```
alluxio.master.mount.table.root.ufs=wasb://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.blob.core.windows.net/<AZURE_DIRECTORY>/
```

其次，将以下属性添加到`conf/alluxio-site.properties`来指定根挂载点的Azure account证书：

```
alluxio.master.mount.table.root.option.fs.azure.account.key.<AZURE_ACCOUNT>.blob.core.windows.net=<YOUR ACCESS KEY>
```

### 嵌套挂载
 Azure blob store位置可以挂载在Alluxio命名空间中的嵌套目录中，以便统一访问到多个底层存储系统。Alluxio的[用户CLI]({{ '/cn/operation/User-CLI.html' | relativize_url }})可以用于此目的。

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.key.<AZURE_ACCOUNT>.blob.core.windows.net=<AZURE_ACCESS_KEY>\
  /mnt/azure wasb://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.blob.core.windows.net/<AZURE_DIRECTORY>/
```

完成这些修改后，Alluxio应该已经配置好以使用Azure Blob Store作为底层存储系统，你可以使用它本地运行Alluxio。

## 使用Azure Blob Store本地运行Alluxio

完成所有配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

```
./bin/alluxio format
./bin/alluxio-start.sh local
```

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

```
./bin/alluxio runTests
```

运行成功后，你可以访问你的容器`<AZURE_CONTAINER>`，确认其中包含了由Alluxio创建的文件和目录。该测试中，创建的文件名应该像下面这样：

```
<AZURE_CONTAINER>/<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

若要停止Alluxio，你可以运行以下命令:

```
./bin/alluxio-stop.sh local
```
