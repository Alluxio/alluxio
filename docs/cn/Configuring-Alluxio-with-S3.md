---
layout: global
title: 在Amazon S3上配置Alluxio
nickname: Alluxio使用S3
group: Under Store
priority: 0
---

该指南介绍如何配置Alluxio从而使用[Amazon S3](https://aws.amazon.com/s3/)作为底层文件系统。

# 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有配置文件，由template文件创建配置文件：

{% include Common-Commands/copy-alluxio-env.md %}

另外，为了在S3上使用Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该bucket里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该向导中，S3 bucket的名称为`S3_BUCKET`，在该bucket里的目录名称为`S3_DIRECTORY`。

# 配置Alluxio

若要在Alluxio中使用S3作为底层文件系统，一定要修改`conf/alluxio-env.sh`配置文件。首先要指定一个**已有的**S3 bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-env.sh`中添加如下语句指定它：

{% include Configuring-Alluxio-with-S3/underfs-address.md %}

接着，需要指定AWS证书以便访问S3，在`conf/alluxio-env.sh`中的`ALLUXIO_JAVA_OPTS`部分添加：

{% include Configuring-Alluxio-with-S3/aws.md %}

其中，`<AWS_ACCESS_KEY_ID>`和`<AWS_SECRET_ACCESS_KEY>`是你实际的[AWS keys](https://aws.amazon.com/developers/access-keys)，或者其他包含证书的环境变量。

更改完成后，Alluxio应该能够将S3作为底层文件系统运行，你可以尝试[使用S3在本地运行Alluxio](#running-alluxio-locally-with-s3)

## 通过代理访问S3

若要通过代理与S3交互，在`conf/alluxio-env.sh`中的`ALLUXIO_JAVA_OPTS`部分添加：

{% include Configuring-Alluxio-with-S3/proxy.md %}

其中，`<PROXY_HOST>`和`<PROXY_PORT>`为代理的主机名和端口，`<USE_HTTPS?>`根据是否使用https与代理通信设置为`true`或`false`。

如果运行Alluxio client的JVM不在Alluxio Master和Workers上，也要为其进行这些设置，参考[配置分布式应用](#configuring-distributed-applications)

# 配置应用

当构建应用使用Alluxio时，你的应用必须包含`alluxio-core-client`模块，如果你使用[maven](https://maven.apache.org/)构建应用，在配置文件中添加以下以来：

{% include Configuring-Alluxio-with-S3/dependency.md %}


## 启用Hadoop S3客户端（代替本地S3客户端）

Alluxio提供了一个本地客户端与S3交互，默认情况下，当S3被配置为底层文件系统时，该本地客户端会被使用。

但可以选择另外一种与S3通信的方式，即由Hadoop提供的S3客户端。为禁用Alluxio S3客户端（并启用Hadoop S3客户端），还需进行额外配置。若你的应用中包含了`alluxio-core-client`模块，要将`alluxio-underfs-s3`排除在外从而禁用本地客户端，并启用Hadoop S3客户端：

{% include Configuring-Alluxio-with-S3/hadoop-s3-dependency.md %}

然而，Hadoop S3客户端需要`jets3t`来启用S3,而默认配置并未添加该依赖，因此，你必须手动添加`jets3t`包依赖。若使用Maven，添加如下代码加入该依赖：

{% include Configuring-Alluxio-with-S3/jets3t-dependency.md %}

`jets3t 0.9.0`适用于Hadoop `2.3.0`，而`jets3t 0.7.1`应适用于更旧版本的Hadoop。要确认支持你的Hadoop版本的具体`jets3t`版本号，可以参考[MvnRepository](http://mvnrepository.com/)。

## 配置分布式应用

如果你使用的Alluxio Client并非运行在Alluxio Master或者Worker上（在其他JVM上），那需要确保为该JVM提供了AWS证书，最简单的方法是在启动client JVM时添加如下选项：

{% include Configuring-Alluxio-with-S3/java-bash.md %}

# 使用S3在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的S3目录`S3_BUCKET/S3_DIRECTORY`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-S3/s3-file.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}
