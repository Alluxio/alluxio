---
layout: global
title: Alluxio集成Kodo作为底层存储
nickname: Alluxio集成Kodo作为底层存储
group: Storage Integrations
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置 Alluxio 以使用[Qiniu Kodo](https://www.qiniu.com/products/kodo)作为底层文件系统。七牛云对象存储( Kodo )是七牛云提供的高可靠、强安全、低成本、可扩展的存储服务。

## 初始步骤

要在多台机器上运行 Alluxio 集群，需要在这些机器上部署二进制包。你可以自己[由Alluxio源码编译二进制包](Building-Alluxio-From-Source.html)，或者[直接下载预编译过的二进制包](Running-Alluxio-Locally.html)。

为了在 Kodo 上使用 Alluxio，需要创建一个 bucket（或者使用一个已有的 bucket ）。在该指南中，Kodo bucket 的名称为KODO_BUCKET，在该 bucket 里的目录名称为 KODO_DIRECTORY。要使用七牛对象存储服务，需要提供一个可供识别指定 bucket 的域名，本向导中为KODO_DOWNLOAD_HOST。

## 安装Kodo

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。 Kodo的挂载点可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装

若要在Alluxio中使用七牛Kodo作为底层文件系统，一定要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的存储空间和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它。

```
alluxio.master.mount.table.root.ufs=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```
七牛不支持直接在管理控制台进行文件夹的管理，但是跟其他云厂商相同，我们默认使用`/`来对文件进行区隔。若配置 KODO_DIRECTORY 后，会在 Kodo 生成 以 KODO_DIRECTORY/ 为前缀的文件。

接着，需要一些配置访问Kodo，在`conf/alluxio-site.properties`中添加:

```
fs.kodo.accesskey=<KODO_ACCESS_KEY>
fs.kodo.secretkey=<KODO_SECRET_KEY>
alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST>
alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT>
```

首先, 你可以从[七牛密钥管理](https://portal.qiniu.com/user/key)中 获取 `AccessKey/SecretKey`。

`alluxio.underfs.kodo.downloadhost` 可以在[七牛云对象存储管理平台](https://portal.qiniu.com/bucket) 中的空间概览中获取。
`alluxio.underfs.kodo.endpoint` 是七牛云存储源站的端点域名配置,可以根据存储空间所在存储区域进行配置:

Kodo 存储空间和对应端点域名可以参考

| 存储区域 | 区域简称 | EndPoint |
| ------- | -------- | --------- |
|华东| z0|  iovip.qbox.me |
|华北| z1| iovip-z1.qbox.me|
|华南| z2| iovip-z2.qbox.me |
|北美| na0| iovip-na0.qbox.me |
|东南亚| as0| iovip-as0.qbox.me |

### 嵌套目录安装

Kodo 可以安装在 Alluxio 命名空间中的嵌套目录中，以统一访问多个存储系统。
[Mount 命令]({{ '/cn/operation/User-CLI.html' | relativize_url }}#mount)可以实现这一目的。例如，下面的命令将Kodo容器内部的目录挂载到 Alluxio的`/kodo`目录:

```console
$ ./bin/alluxio fs mount --option fs.kodo.accesskey=<KODO_ACCESS_KEY> \
  --option fs.kodo.secretkey=<KODO_SECRET_KEY> \
  --option alluxio.underfs.kodo.downloadhost=<KODO_DOWNLOAD_HOST> \
  --option alluxio.underfs.kodo.endpoint=<KODO_ENDPOINT> \
  /kodo kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```

## 使用KODO在本地运行Alluxio

配置完成后，你可以在本地启动 Alluxio，观察一切是否正常运行:

```console
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local
```

该命令应当会启动一个 Alluxio master 和一个 Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看 master UI。

接着，你可以运行一个简单的示例程序:

```console
$ ./bin/alluxio runTests
```

运行成功后，访问你的 Kodo 目录`KODO://<KODO_BUCKET>/<KODO_DIRECTORY>`，确认其中包含了由 Alluxio 创建的文件和目录。在该测试中，创建的文件名称应像`KODO_BUCKET/KODO_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`这样。

运行以下命令停止 Alluxio:

```console
$ ./bin/alluxio-stop.sh local
```
