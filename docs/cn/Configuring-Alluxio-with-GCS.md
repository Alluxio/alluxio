---
layout: global
title: 在GCS上配置Alluxio 
nickname: Alluxio使用GCS 
group: Under Store
priority: 0
---

该指南介绍如何配置Alluxio以使用[Google Cloud Storage](https://cloud.google.com/storage/)作为底层文件系统。

# 初始步骤

首先，在你的机器上必须安装Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有生成配置文件，可以由template文件创建配置文件：

{% include Common-Commands/copy-alluxio-env.md %}

而且，为了在Alluxio上使用GCS, 需要创建一个bucket（或者使用一个已有的bucket)。你应该注意你在这个bucket里准备使用的目录，你可以自己在这个bucket里面创建一个新目录，或者使用一个已有的目录。在这个指南中，我们将GCS bucket取名为`GCS_BUCKET`，bucket中的目录取名为`GCS_DIRECTORY`。

如果你刚接触GCS，请先阅读GCS[文档](https://cloud.google.com/storage/docs/overview)。

# 配置Alluxio

为了配置Alluxio以使用GCS作为其底层文件系统，需要修改`conf/alluxio-env.sh`文件。第一个需要修改的地方就是指定一个已有的bucket以及bucket中的目录作为底层文件系统。你可以通过向`conf/alluxio-env.sh`文件添加以下代码来指定：

{% include Configuring-Alluxio-with-GCS/underfs-address.md %}

接下来，你需要指定Google证书来接入GCS。在`conf/alluxio-env.sh`文件的
`ALLUXIO_JAVA_OPTS`部分添加:

{% include Configuring-Alluxio-with-GCS/google.md %}

在这里 `<GCS_ACCESS_KEY_ID>` 和 `<GCS_SECRET_ACCESS_KEY>`应该用你的
[GCS可互操作存储的access keys](https://console.cloud.google.com/storage/settings)来替换,或者用其他包含你的证书的环境变量来替换。

注意: GCS的可互操作性默认是未启用的。请在[GCS 设置](https://console.cloud.google.com/storage/settings) 中点击可互操作按键来启用可互操作性。然后点击`创建一个新的key`来获得the Access Key和Secret pair.

执行完以上步骤后，Alluxio应该已经配置GCS作为其底层文件系统，然后你可以尝试[使用GCS本地运行Alluxio](#running-alluxio-locally-with-gcs).

# 配置你的应用

当使用Alluxio构建你的应用时，你的应用需要包含`alluxio-core-client`模块。如果你正在使用 [maven](https://maven.apache.org/)，你可以通过添加以下代码来添加你的应用的依赖：

{% include Configuring-Alluxio-with-GCS/dependency.md %}

## 配置分布式应用

如果你使用的Alluxio client并非运行在Alluxio Master 或者Workers上（在其他的JVM上），那需要确保为该JVM提供了Google证书。最简单的方法是在启动client JVM时添加如下选项。例如:

{% include Configuring-Alluxio-with-GCS/java-bash.md %}

# 使用GCS本地运行Alluxio

完成所有的配置之后，你可以本地运行Alluxio,观察是否一切运行正常。

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的GCS目录`GCS_BUCKET/GCS_DIRECTORY`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-GCS/gcs-file.md %}

运行以下命令停止Alluxio:

{% include Common-Commands/stop-alluxio.md %}
