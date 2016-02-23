---
layout: global
title: 在OSS上配置Alluxio
nickname: Alluxio使用OSS
group: Under Store
priority: 4
---

该向导介绍如何配置Alluxio从而使用[Aliyun OSS](http://www.aliyun.com/product/oss/?lang=en)作为底层文件系统。对象存储服务（OSS）是阿里云提供的一个大容量、安全、高可靠性的云存储服务。

## 初始步骤

要在许多机器上运行Alluxio集群，需要在这些机器上部署二进制文件。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，由template文件创建配置文件：

{% include Common-Commands/copy-alluxio-env.md %}

另外，为了在OSS上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该向导中，OSS bucket的名称为OSS_BUCKET，在该bucket里的目录名称为OSS_DIRECTORY。另外，要使用OSS服务，还需提供一个oss 端点，该端点指定了你的bucket在哪个范围，本向导中的端点名为OSS_ENDPOINT。要了解更多指定范围的端点的具体内容，可以参考[这里](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region)，要了解更多OSS Bucket的信息，请参考[这里](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/function&bucket)

## 配置Alluxio

若要在Alluxio中使用OSS作为底层文件系统，一定要修改`conf/alluxio-env.sh`配置文件。首先要指定一个已有的OSS bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-env.sh`中添加如下语句指定它：

{% include Configuring-Alluxio-with-OSS/underfs-address.md %}

接着，需要指定Aliyun证书以便访问OSS，在`conf/alluxio-env.sh`中的`ALLUXIO_JAVA_OPTS`部分添加：

{% include Configuring-Alluxio-with-OSS/oss-access.md %}

其中，`<OSS_ACCESS_KEY_ID>`和`<OSS_SECRET_ACCESS_KEY>`是你实际的[Aliyun keys](https://ak-console.aliyun.com/#/accesskey)，或者其他包含证书的环境变量，你可以从[这里](http://intl.aliyun.com/docs#/pub/oss_en_us/product-documentation/domain-region)获取你的`<OSS_ENDPOINT>`。

如果你不太确定如何更改`conf/alluxio-env.sh`，有另外一个方法提供这些配置。可以在`conf/`目录下创建一个`alluxio-site.properties`文件，并在其中添加：

{% include Configuring-Alluxio-with-OSS/properties.md %}

更改完成后，Alluxio应该能够将OSS作为底层文件系统运行，你可以尝试[使用OSS在本地运行Alluxio](#running-alluxio-locally-with-s3)

## 配置分布式应用

如果你使用的Alluxio client并非运行在Alluxio Master或者Worker上（在其他JVM上），那需要确保为该JVM提供了Aliyun证书，最简单的方法是在启动client JVM时添加如下选项：

{% include Configuring-Alluxio-with-OSS/java-bash.md %}

## 使用OSS在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察是否正确运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的OSS目录OSS_BUCKET/OSS_DIRECTORY，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-OSS/oss-file.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}
