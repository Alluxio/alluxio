---
layout: global
title: Azure Storage Gen2
nickname: Azure Data Lake Storage Gen2 
group: Storage Integrations
priority: 2
---

* Table of Contents
{:toc}

本指南介绍如何配置 Alluxio，使其与底层存储系统 [Azure Data Lake Storage Gen2](https://learn.microsoft.com/zh-cn/azure/storage/blobs/data-lake-storage-introduction) 一起运行。

## 部署条件

电脑上应已安装好 Alluxio 程序。如果没有安装，可[编译Alluxio源代码]({{ '/cn/contributor/Building-Alluxio-From-Source.html' | relativize_url }}),
或直接 [下载已编译好的Alluxio程序]({{ '/cn/deploy/Running-Alluxio-Locally.html' | relativize_url }}).

在将 Azure 数据湖存储与 Alluxio 一起运行前，请在 [Azure 帐户中创建一个新的 Data Lake Storage](https://learn.microsoft.com/zh-cn/azure/storage/blobs/create-data-lake-storage-account) 或使用现有的 Data Lake Storage。这里还应指定需使用的 directory（目录），创建一个新的目录或使用现有目录均可。此外，还需要一个[共享密钥](https://learn.microsoft.com/zh-cn/rest/api/storageservices/authorize-with-shared-key)。
本指南中的 Azure 存储帐户名为 `<AZURE_ACCOUNT>`，该存储帐户中的目录 `<AZURE_DIRECTORY>`, container（容器）名为 `<AZURE_CONTAINER>`.

## 通过共享密钥配置

### 根挂载

如果要使用 Azure Data Lake Storage 作为 Alluxio 根挂载点的 UFS，需要通过修改 `conf/alluxio-site.properties` 来配置 Alluxio，使其可访问底层存储系统。如果该配置文件不存在，可通过模板创建。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

修改 `conf/alluxio-site.properties` 来指定 UFS 地址，需包括：

```properties
alluxio.master.mount.table.root.ufs=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

通过在 `conf/alluxio-site.properties` 中添加以下属性来指定共享密钥：

```properties
alluxio.master.mount.table.root.option.fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<SHARED_KEY>
```

### 嵌套挂载
Azure Data Lake 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的 [Command Line Interface]({{ '/cn/operation/User-CLI.html' | relativize_url }})（命令行）来进行挂载。

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.key.<AZURE_ACCOUNT>.dfs.core.windows.net=<SHARED_KEY> \
  /mnt/abfs abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

在完成这些修改之后，Alluxio 已经配置完毕，可以与底层存储 Azure Data Lake 一起在本地运行。

## 通过 OAuth 2.0 客户端凭证配置

### 根挂载

如果要使用 Azure Data Lake Storage 作为 Alluxio 根挂载点的 UFS，需要通过修改 `conf/alluxio-site.properties` 来配置 Alluxio，使其可访问底层存储系统。如果该配置文件不存在，可通过模板创建。

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

修改 `conf/alluxio-site.properties` 来指定 UFS 地址，需包括：

```properties
alluxio.master.mount.table.root.ufs=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

通过在 `conf/alluxio-site.properties` 中添加以下属性来指定 OAuth 2.0 客户端凭证（注意 URL 的 Endpoint 应使用 V1 token Endpoint）：


```properties
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.id=<CLIENT_ID>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.secret=<CLIENT_SECRET>
```

### 嵌套挂载

Azure Data Lake 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的 [Command Line Interface]({{ '/cn/operation/User-CLI.html' | relativize_url }})（命令行）来进行挂载。

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.oauth2.client.endpoint=<OAUTH_ENDPOINT> \
  --option fs.azure.account.oauth2.client.id=<CLIENT_ID> \
  --option fs.azure.account.oauth2.client.secret=<CLIENT_SECRET> \
  /mnt/abfs abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

在完成这些修改之后，Alluxio 已经配置完毕，可以与底层存储 Azure Data Lake 一起在本地运行。

## 通过 Azure 托管身份服务配置

### 根挂载

如果要使用 Azure Data Lake Storage 作为 Alluxio 根挂载点的 UFS，需要通过修改 `conf/alluxio-site.properties` 来配置 Alluxio，使其可访问底层存储系统。如果该配置文件不存在，可通过模板创建。
template.

```console
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

修改 `conf/alluxio-site.properties` 来指定UFS 地址，需包括：

```properties
alluxio.master.mount.table.root.ufs=abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

通过在 `conf/alluxio-site.properties` 中添加以下属性来指定 Azure 托管身份：

```properties
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.msi.endpoint=<MSI_ENDPOINT>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.client.id=<CLIENT_ID>
alluxio.master.mount.table.root.option.fs.azure.account.oauth2.msi.tenant=<TENANT>
```

### 嵌套挂载
Azure Data Lake 存储位置可以挂载在 Alluxio 命名空间中的嵌套目录下，以便统一访问多个底层存储系统。可使用 Alluxio 的 [Command Line Interface]({{ '/cn/operation/User-CLI.html' | relativize_url }})（命令行）来进行挂载。

```console
$ ./bin/alluxio fs mount \
  --option fs.azure.account.oauth2.msi.endpoint=<MSI_ENDPOINT> \
  --option fs.azure.account.oauth2.client.id=<CLIENT_ID> \
  --option fs.azure.account.oauth2.msi.tenant=<TENANT> \
  /mnt/abfs abfs://<AZURE_CONTAINER>@<AZURE_ACCOUNT>.dfs.core.windows.net/<AZURE_DIRECTORY>/
```

在完成这些修改之后，Alluxio 已经配置完毕，可以与底层存储 Azure Data Lake 一起在本地运行。

## 将 Alluxio 与 Data Lake Storage 一起在本地运行

在本地启动Alluxio，检查是否一切运行正常。

```console
./bin/alluxio format
./bin/alluxio-start.sh local
```

该命令会启动一个 Alluxio master 和一个 Alluxio worker。可通过 [http://localhost:19999](http://localhost:19999) 查看 master UI。

运行一个简单的示例程序：

```console
./bin/alluxio runTests
```

访问目录 `<AZURE_DIRECTORY>`，以验证 Alluxio 创建的文件和目录是否存在。就本次测试而言，将看到如下的文件：

```
<AZURE_DIRECTORY>/default_tests_files/BASIC_CACHE_PROMOTE_CACHE_THROUGH
```

要终止Alluxio, 可运行以下命令：

```console
./bin/alluxio-stop.sh local
```
