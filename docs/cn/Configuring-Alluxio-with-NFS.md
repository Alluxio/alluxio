---
layout: global
title: 在NFS上配置Alluxio
nickname: Alluxio使用NFS
group: Under Store
priority: 5
---

* 内容列表
{:toc}

该指南介绍如何配置Alluxio以使用[NFS](http://nfs.sourceforge.net)作为底层文件系统。

## 初始步骤

首先，本地要有Alluxio二进制包。你可以自己[编译Alluxio](Building-Alluxio-Master-Branch.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

## 配置Alluxio

您需要修改`conf/alluxio-site.properties`配置Alluxio，以使用NFS作为其底层存储系统。如果该配置文件不存在，请从模板创建该配置文件。

```bash
$ cp conf/alluxio-site.properties.template conf/alluxio-site.properties
```

假定所有NFS客户端与Alluxio部署在同样的节点上，且NFS挂载在`/mnt/nfs`，那以下的环境变量要添加到`conf/alluxio-site.properties`配置文件中：

```
properties
alluxio.underfs.address=/mnt/nfs
```

## 使用NFS运行Alluxio

配置完成后，你可以在本地启动Alluxio，观察一切是否正常运行：

{% include Configuring-Alluxio-with-NFS/start-alluxio.md %}

该命令应当会启动一个Alluxio master和一个Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Configuring-Alluxio-with-NFS/runTests.md %}

运行成功后，访问你的NFS volume，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Alluxio-with-NFS/nfs-file.md %}

运行以下命令停止Alluxio：

{% include Configuring-Alluxio-with-NFS/stop-alluxio.md %}
