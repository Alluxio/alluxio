---
layout: global
title: 在GlusterFS上配置Alluxio
nickname: Alluxio使用GlusterFS
group: Under Store
priority: 2
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[GlusterFS](http://www.gluster.org/)作为底层文件系统。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

然后，如果你还没有去配置，你可以通过 `bootstrapConf` 命令进行配置。例如：如果你在本地运行 Alluxio ，你要把 `ALLUXIO_MASTER_HOSTNAME` 设置为 `localhost`。

{% include Configuring-Alluxio-with-GlusterFS/bootstrapConf.md %}

或者，你也可以利用template文件创建配置文件并且手动设置配置内容。

{% include Common-Commands/copy-alluxio-env.md %}

## 配置Alluxio

假定GlusterFS bricks与Alluxio部署在同样的节点上，且GlusterFS volume挂载在`/alluxio_vol`，那以下的环境变量要添加到`conf/alluxio-site.properties`配置文件中：

{% include Configuring-Alluxio-with-GlusterFS/underfs-address.md %}

## 使用GlusterFS在本地运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察是否一切运行正常

{% include Common-Commands/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Common-Commands/runTests.md %}

运行成功后，访问你的GlusterFS volume，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-GlusterFS/glusterfs-file.md %}

运行以下命令停止Alluxio：

{% include Common-Commands/stop-alluxio.md %}
