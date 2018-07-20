---
layout: global
title: 在Minio上配置Alluxio
nickname: Alluxio使用Minio
group: Under Store
priority: 0
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[Minio](https://minio.io/)作为底层存储系统。
Alluxio本地提供了s3a:// scheme(建议使用以获取更好的性能)。您可以使用此方案连接Alluxio与Minio服务器。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Minio

您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用Minio作为其底层存储系统。如果该配置文件不存在，请从模板创建该配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

Minio是为云应用程序和DevOps构建的对象存储服务器。 Minio提供了AWS S3的开源替代方案。

按照[这里](http://docs.minio.io/docs/minio-quickstart-guide)的步骤启动Minio服务器实例。
然后创建一个桶(或使用现有的桶)。一旦服务器启动，请记录Minio服务器端点，accessKey和secretKey。

您还应该留意希望在该桶中使用的目录，通过创建一个新目录或使用已经存在的目录。
就本指南而言，Minio桶命名为`MINIO_BUCKET`，该桶中的目录命名为`MINIO_DIRECTORY`。

## 配置Alluxio

您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用Minio作为其底层存储系统。
首先需要指定**已存在的**Minio桶和目录作为底层存储系统

在`conf/alluxio-site.properties`文件中要修改的所有字段如下所示：

```properties
alluxio.underfs.address=s3a://<MINIO_BUCKET>/<MINIO_DIRECTORY>
alluxio.underfs.s3.endpoint=http://<MINIO_ENDPOINT>/
alluxio.underfs.s3.disable.dns.buckets=true
alluxio.underfs.s3a.inherit_acl=false
aws.accessKeyId=<MINIO_ACCESS_KEY_ID>
aws.secretKey=<MINIO_SECRET_KEY_ID>
```

对于这些参数，用您的Minio服务的主机名和端口替换`<MINIO_ENDPOINT>`，例如`http://localhost:9000`。
如果端口值未设置，对于`http`默认端口为80，对于`https`默认端口为443。
