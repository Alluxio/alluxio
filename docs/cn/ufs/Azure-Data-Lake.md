---
layout: global
title: Azure 数据湖存储
nickname: Azure 数据湖存储
group: Storage Integrations
priority: 2
---

* Table of Contents
{:toc}

本指南介绍如何配置Alluxio，使其与底层存储系统 [Azure Data Lake Storage Gen1](https://docs.microsoft.com/en-in/azure/data-lake-store/data-lake-store-overview) 一起运行。

## 部署条件

电脑上应已安装好Alluxio程序。如果没有安装，可[编译Alluxio源代码]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
或直接[下载已编译好的Alluxio程序]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

在将 Azure 数据湖存储与 Alluxio 一起运行前，请在 Azure 帐户中创建一个新的 Data Lake Storage 或使用现有的 Data Lake Storage。这里还应指定需使用的 directory（目录），创建一个新的 directory 或使用现有 directory 均可。此外，还需为存储帐户设置[服务到服务验证](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory)。本指南中的 Azure 存储帐户名为 `<AZURE_ACCOUNT>`，该存储帐户中的 directory 名为 `<AZURE_DIRECTORY>`。 要了解有关 Azure 存储帐户的更多信息，请点击[此处](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-get-started-portal).


## 基本设置

### 根挂载

如果要使用 Azure Data Lake Storage 作为 Alluxio 根挂载点的 UFS，则需要通过修改 `conf/alluxio-site.properties` 来配置Alluxio，使其可访问底层存储系统。如果该配置文件不存在，可通过模板创建。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

修改 `conf/alluxio-site.properties` 来指定UFS 地址，需包括：

```properties
alluxio.master.mount.table.root.ufs=adl://<AZURE_ACCOUNT>.azuredatalakestore.net/<AZURE_DIRECTORY>/
```

通过在 `conf/alluxio-site.properties`中添加以下属性，来指定用于根挂载点的 Azure 帐户的 Azure AD Application 的应用ID、身份验证密钥和租户 ID：
- 有关如何获取应用 ID 和身份验证密钥（也称为客户端密钥）的说明，请参见 [Get application ID and authentication key](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#get-tenant-and-app-id-values-for-signing-in)。
- 有关如何获取租户ID 的说明，请参见 [Get tenant ID](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal#get-tenant-and-app-id-values-for-signing-in).

```properties
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.client.id=<APPLICATION_ID>
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.credential=<AUTHENTICATION_KEY>
alluxio.master.mount.table.root.option.fs.adl.account.<AZURE_ACCOUNT>.oauth2.refresh.url=https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
```

### 嵌套挂载
Azure Data Lake 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的 [Command Line Interface]({{ '/cn/operation/User-CLI.html' | relativize_url }})（命令行） 来进行挂载。

```console
$ ./bin/alluxio fs mount \
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.client.id=<APPLICATION_ID>
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.credential=<AUTHENTICATION_KEY>
  --option fs.adl.account.<AZURE_ACCOUNT>.oauth2.refresh.url=https://login.microsoftonline.com/<TENANT_ID>/oauth2/token
  /mnt/adls adl://<AZURE_ACCOUNT>.azuredatalakestore.net/<AZURE_DIRECTORY>/
```

在完成这些修改之后，Alluxio 已经配置完毕，可以与底层存储 Azure Data Lake 一起在本地运行。

## 将 Alluxio 与 Data Lake Storage 一起在本地运行

在本地启动Alluxio，检查是否一切运行正常。

```console
./bin/alluxio format
./bin/alluxio-start.sh local
```

该命令会启动一个 Alluxio master 和一个 Alluxio worker。可通过
[http://localhost:19999](http://localhost:19999) 查看 master UI。

运行一个简单的示例程序：

```console
./bin/alluxio runTests
```

访问 directory `<AZURE_DIRECTORY>` 以验证 Alluxio 创建的文件和目录是否存在。就本次测试而言，将看到如下文件：

```
<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

要终止Alluxio, 可运行以下命令：

```console
./bin/alluxio-stop.sh local
```
