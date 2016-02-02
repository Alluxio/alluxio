---
layout: global
group: Features
title: Web界面
priority: 6
---

* Table of Contents
{:toc}

Tachyon提供了用户友好的Web界面以便用户查看和管理。master和worker都拥有各自的Web UI。master Web界面的默认
端口是19999，worker的端口是30000。

# Tachyon Master Web界面

Tachyon master提供了Web界面以便用户管理。Tachyon master Web界面的默认端口是19999，访问
`http://<MASTER IP>:19999`即可查看。举个例子，如果你在本地启动Tachyon，访问
[localhost:19999](http://localhost:19999)即可查看master Web界面。

Tachyon master Web界面包含若干不同页面，描述如下：

## 主页

Tachyon master 主页如下所示:

![Tachyon Master Home Page]({{site.data.img.screenshot_overview}})

主页显示了系统状态的概要信息，包括以下部分：

* **Tachyon概要**

    Tachyon系统级信息

* **集群使用概要**

    Tachyon存储信息和底层存储信息。Tachyon的存储使用可以接近100%，但底层的存储使用不应该接近100%。

* **存储使用概要**

   Tachyon分层存储信息，分项列出了Tachyon集群中每层存储空间的使用量。

## 配置页面

查看当前系统的配置信息，点击屏幕上方导航栏的"System Configuration"。

![configurations]({{site.data.img.screenshot_systemConfiguration}})

配置页面包含两部分:

* **Tachyon配置**

     所有Tachyon配置的属性和设定值的映射。

* **白名单**

    包含所有符合要求的可以存储在Tachyon上的Tachyon路径前缀。用户可以访问路径前缀不在白名单上的文件。只有白名单中的文件可以存储在Tachyon上。

## 浏览文件系统页面

你可以通过Web UI浏览Tachyon文件系统。当你点击导航栏上的"Browse File System"选项卡，你可以看到如下的页面：

![browse]({{site.data.img.screenshot_browseFileSystem}})

当前文件夹的文件会被列出，包括文件名，文件大小，块大小，内存中数据的百分比，创建时间和修改时间。查看文件内容只需点击对应的文件即可。

## 浏览内存中文件页面

浏览所有内存中文件，点击导航栏"In-Memory Files"选项卡。

![inMemFiles]({{site.data.img.screenshot_inMemoryFiles}})

当前在内存存储层的文件会被列出，包括文件名，文件大小，块大小，文件是否被固定在内存中，文件创建时间和文件修改时间。

## Workers页面

master也显示了系统中所有已知的Tachyon worker。点击"Workers"选项卡即可查看。

![workers]({{site.data.img.screenshot_workers}})

workers页面将所有Tachyon worker节点分为两类显示：

* **活动节点**

    所有当前可以处理Tachyon请求的节点列表。点击worker名将重定向到worker的web UI。

* **失效节点**

    所有被master宣布失效的worker列表，通常是因为等待worker心跳超时，可能的原因包括worker系统重启或网络故障。

# Tachyon Workers Web界面

每个Tachyon worker也提供Web界面显示worker信息。worker Web界面的默认端口是30000，访问
`http://<WORKER IP>:30000`即可查看。举个例子，如果你在本地启动Tachyon，访问
[localhost:30000](http://localhost:30000)即可查看worker Web界面。

## 主页

Tachyon worker的主页和Tachyon master类似，但是显示的是单个worker的特定信息。因此，它有类似的部分：
**Worker 概要**, **存储使用概要**, **分层存储详细信息**.

## 块信息页面

在块信息页面，可以看到worker上的文件，以及其他信息，如：文件大小和文件所在的存储层。当你点击文件时，可以看
到文件的所有块信息。
