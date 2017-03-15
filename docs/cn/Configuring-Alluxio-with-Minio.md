---
layout: global
title: 在Minio上配置Alluxio
nickname: Minio使用Alluxio
group: Under Store
priority: 0
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Minio](https://minio.io/)作为底层存储系统。
Alluxio本地提供了s3a:// scheme(建议使用以获取更好的性能)。您可以使用此方案连接Alluxio与Minio服务器。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有配置文件，可通过`bootstrapConf`命令创建。
例如，如果你是在本机运行Alluxio，那么`<ALLUXIO_MASTER_HOSTNAME>`应该设置为`localhost`。

{% include Configuring-Alluxio-with-Minio/bootstrapConf.md %}

除了上述方式，也可以通过template文件创建配置文件，并且手动设置相应参数。

{% include Common-Commands/copy-alluxio-env.md %}

## 配置Minio

Minio是为云应用程序和DevOps构建的对象存储服务器。 Minio提供了AWS S3的开源替代方案。

按照[这里](http://docs.minio.io/docs/minio-quickstart-guide)的步骤启动Minio服务器实例。
然后创建一个桶(或使用现有的桶)。一旦服务器启动，请记录Minio服务器端点，accessKey和secretKey。

您还应该留意希望在该桶中使用的目录，通过创建一个新目录或使用已经存在的目录。
就本指南而言，Minio桶命名为`MINIO_BUCKET`，该桶中的目录命名为`MINIO_DIRECTORY`。

## 配置Alluxio

您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用Minio作为其底层存储系统。
首先需要指定**已存在的**Minio桶和目录作为底层存储系统

在`conf/alluxio-site.properties`文件中要修改的所有字段如下所示：

{% include Configuring-Alluxio-with-Minio/minio.md %}

对于这些参数，用您的Minio服务的URL和端口替换`<MINIO_ENDPOINT>`和`<MINIO_PORT>`。

用`true`或`false`替换`<USE_HTTPS>`。
如果使用`true`(使用HTTPS)，还需要用提供者的HTTPS端口替换`<MINIO_HTTPS_PORT>`，并且删除`alluxio.underfs.s3.endpoint.http.port`参数。
如果您使用`false`来替换`<USE_HTTPS>`(使用HTTP)，同样需要用提供者的HTTPS端口替换`<MINIO_HTTP_PORT>`，并且删除`alluxio.underfs.s3.endpoint.https.port`参数。
如果HTTP或HTTPS端口值未设置，`<HTTP_PORT>`默认端口为80，`<HTTPS_PORT>`默认端口为443。
