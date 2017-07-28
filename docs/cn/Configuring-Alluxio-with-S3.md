---
layout: global
title: 在Amazon S3上配置Alluxio
nickname: Alluxio使用S3
group: Under Store
priority: 0
---

* 内容列表
{:toc}

该指南介绍配置[Amazon S3](https://aws.amazon.com/s3/)作为Alluxio底层文件系统的指令。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有进行这项配置，那么用`bootstrapConf`命令创建你的配置文件

例如，如果你在本地机器上运行Alluxio，`ALLUXIO_MASTER_HOSTNAME`应该设置为`localhost`。

{% include Configuring-Alluxio-with-S3/bootstrapConf.md %}

或者，你也可以由template文件创建配置文件并手动设置配置内容：

{% include Common-Commands/copy-alluxio-env.md %}

另外，为了在S3上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该向导中，S3 bucket的名称为`S3_BUCKET`，在该bucket里的目录名称为`S3_DIRECTORY`。

## 配置Alluxio

若要在Alluxio中使用S3作为底层文件系统，一定要修改`conf/alluxio-env.sh`配置文件。首先要指定一个**已有的**S3 bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-env.sh`中添加如下语句指定它：

{% include Configuring-Alluxio-with-S3/underfs-address-s3n.md %}

接着，需要指定AWS证书以便访问S3，在`conf/alluxio-env.sh`中的`ALLUXIO_JAVA_OPTS`部分添加：

{% include Configuring-Alluxio-with-S3/aws.md %}

其中，`<AWS_ACCESS_KEY_ID>`和`<AWS_SECRET_ACCESS_KEY>`是你实际的[AWS keys](https://aws.amazon.com/developers/access-keys)，或者其他包含证书的环境变量。

底层的S3软件库JetS3t可以包含与其请求的主机名的DNS相兼容的bucket名称。你可以选择性地通过配置`conf/alluxio-env.sh`文件中的`ALLUXIO_JAVA_OPTS`部分的内容来指定这种行为。具体地，添加下面内容：

{% include Configuring-Alluxio-with-S3/jets3t.md %}

当`<DISABLE_DNS>`设置为`false`（默认情况）时，定向到名称为"mybucket"的bucket会被发送到名称为"mybucket.s3.amazonaws.com"的主机。当当`<DISABLE_DNS>`设置为`true`时，JetS3t会在HTTP消息请求路径中设定bucket名称，例如"http://s3.amazonaws.com/mybucket"，而不是在主机头部设定。如果不设置该参数，系统将默认设置为`false`.更多的详情请参考http://www.jets3t.org/toolkit/configuration.html。

更改完成后，Alluxio应该能够将S3作为底层文件系统运行，你可以尝试[使用S3在本地运行Alluxio](#running-alluxio-locally-with-s3)

### 通过代理访问S3

若要通过代理与S3交互，在`conf/alluxio-env.sh`中的`ALLUXIO_JAVA_OPTS`部分添加：

{% include Configuring-Alluxio-with-S3/proxy.md %}

其中，`<PROXY_HOST>`和`<PROXY_PORT>`为代理的主机名和端口，`<USE_HTTPS?>`根据是否使用https与代理通信设置为`true`或`false`。

## 配置应用依赖

当使用Alluxio构建你的应用时，你的应用需要包含一个client模块，如果要使用[Alluxio file system interface](File-System-API.html)，那么需要配置`alluxio-core-client-fs`模块，如果需要使用[Hadoop file system interface](https://wiki.apache.org/hadoop/HCFS)，则需要使用`alluxio-core-client-hdfs`模块。
举例来说，如果你正在使用 [maven](https://maven.apache.org/)，你可以通过添加以下代码来添加你的应用的依赖：

{% include Configuring-Alluxio-with-S3/dependency.md %}

另外，你也可以将`conf/alluxio-site.properties`（包含了设置证书的配置项）拷贝到你的应用运行时的classpath中（例如对于Spark而言为`$SPARK_CLASSPATH`），或者将该配置文件所在的路径追加到classpath末尾。

### 使用非亚马逊服务提供商

如果需要使用一个不是来自"s3.amazonaws.com"的S3服务，用户需要修改`conf/alluxio-env.sh`文件中的`ALLUXIO_JAVA_OPTS`部分，具体添加如下：

{% include Configuring-Alluxio-with-S3/non-amazon.md %}

对于这些参数，将`<S3_ENDPOINT>`参数替换成你的S3服务的主机名。该参数只有在你的服务提供商非`s3.amazonaws.com`时才需要进行配置。

将`<USE_HTTPS>`设置为`true`或者`false`。如果设置为`true` (使用HTTPS)，同时需要设置`<HTTPS_PORT>`为服务提供商给定的HTTPS port，并且删除`alluxio.underfs.s3.endpoint.http.port`参数。 如果你将`<USE_HTTPS>`设置为`false`（即使用HTTP），同时也需要设置`<HTTPS_PORT>`为服务提供商给定的HTTPS port，并且删除`alluxio.underfs.s3.endpoint.https.port`参数。如果HTTP或HTTPS的port值没有设定，那么`<HTTP_PORT>`采用的默认端口为80，`<HTTPS_PORT>`采用的默认端口为443.

### 使用v2的S3签名

一些S3服务提供商仅仅支持v2签名。对这些S3提供商来说，可以通过设置`alluxio.underfs.s3a.signer.algorithm`为`S3SignerType`来强制使用v2签名。

## 使用S3在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的S3目录`S3_BUCKET/S3_DIRECTORY`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-S3/s3-file.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}

## S3访问控制

如果Alluxio安全认证被启用，Alluxio将会遵循底层对象存储的访问权限控制。

Alluxio配置中指定的S3证书代表一个S3用户，S3服务终端在一个用户访问bucket和对象时将会检查该用户的权限，如果该用户对某个bucket没有足够的访问权限，将会抛出一个权限错误。当Alluxio安全认证被启用时，Alluxio在第一次从底层S3将元数据加载到Alluxio命名空间时便会同时将其bucket的ACL也加载到Alluxio访问权限元数据中。

### S3用户到Alluxio文件所有者的映射关系

默认情况下，Alluxio会尝试从S3证书中解析其S3用户名。另外，也可以配置`alluxio.underfs.s3.owner.id.to.username.mapping`从而指定某个S3编号到Alluxio用户名的映射关系，其配置形式为"id1=user1;id2=user2"。AWS S3规范编号可以在[该控制台地址](https://console.aws.amazon.com/iam/home?#security_credential)找到，请展开`Account Identifiers`选项卡，并参考"Canonical User ID"。

### S3 ACL到Alluxio权限的映射关系

Alluxio通过S3 bucket的读写ACL来决定Alluxio文件的所有者权限模式。举例来说，如果一个S3用户对某个底层S3 bucket具有只读权限，那么该挂载的目录以及文件的权限模式为0500,如果该S3用户具有所有权限，那么其Alluxio权限模式将为0700。

### 挂载点共享

如果你想在Alluxio命名空间中与其他用户共享S3挂载点，可以启用`alluxio.underfs.object.store.mount.shared.publicly`。

### 权限更改

注意，对Alluxio目录或者文件进行chown/chgrp/chmod等操作不会对其底层S3 bucket或者对象的权限做出更改。
