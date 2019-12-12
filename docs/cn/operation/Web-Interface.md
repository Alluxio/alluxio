---
layout: global
title: Web界面
group: Operations
priority: 3
---

* 内容列表
{:toc}

Alluxio提供了用户友好的Web界面以便用户查看和管理。master和worker都拥有各自的Web UI页面。master Web界面的默认
端口是19999，worker的端口是30000。

## Alluxio Master Web界面

Alluxio master提供了Web界面以便用户管理。Alluxio master Web界面的默认端口是19999，访问
`http://<MASTER IP>:19999`即可查看。举个例子，如果你在本地启动Alluxio，访问
[localhost:19999](http://localhost:19999)即可查看master Web界面。

Alluxio master Web界面包含若干不同页面，描述如下：

### 主页

Alluxio master 主页如下所示:

![Alluxio Master Home Page]({{ '/img/screenshot_overview.png' | relativize_url }})

主页显示了系统状态的概要信息，包括以下部分：

* **Alluxio概要** Alluxio系统级信息

* **集群使用概要** Alluxio存储信息和底层存储信息。Alluxio的存储使用可以接近100%，但底层的存储使用不应该接近100%。

* **存储使用概要** Alluxio分层存储信息，分项列出了Alluxio集群中每层存储空间的使用量。

### 配置页面

查看当前的配置信息，点击屏幕上方导航栏的"Configuration"。

![configurations]({{ '/img/screenshot_systemConfiguration.png' | relativize_url }})

配置页面包含两部分:

* **Alluxio配置** 所有Alluxio配置的属性和设定值的映射。

* **白名单** 包含所有符合要求的可以存储在Alluxio上的Alluxio路径前缀。用户可以访问路径前缀不在白名单上的文件。只有白名单中的文件可以存储在Alluxio上。

### 浏览文件系统

你可以通过Web UI浏览Alluxio文件系统。当你点击导航栏上的"Browse"选项卡，你可以看到如下的页面：

![browse]({{ '/img/screenshot_browseFileSystem.png' | relativize_url }})

当前文件夹的文件会被列出，包括文件名，文件大小，块大小，内存中数据的百分比，创建时间和修改时间。查看文件内容只需点击对应的文件即可。

![viewFile]({{ '/img/screenshot_viewFile.png' | relativize_url }})

### 浏览内存中文件页面

浏览所有内存中文件，点击导航栏"In-Alluxio Files"选项卡。

![inMemFiles]({{ '/img/screenshot_inMemoryFiles.png' | relativize_url }})

当前在Alluxio的文件会被列出，包括文件名，文件大小，块大小，文件是否被固定在内存中，文件创建时间和文件修改时间。

### Workers页面

master也显示了系统中所有已知的Alluxio worker。点击"Workers"选项卡即可查看。

![workers]({{ '/img/screenshot_workers.png' | relativize_url }})

workers页面将所有Alluxio worker节点分为两类显示：

* **存活节点** 所有当前可以处理Alluxio请求的节点列表。点击worker名将重定向到worker的web UI页面。

* **失效节点** 所有被master宣布失效的worker列表，通常是因为等待worker心跳超时，可能的原因包括worker系统重启或网络故障。

### Master度量信息

点击导航栏中的“Metrics”选项卡即可浏览master的度量信息。

![masterMetrics]({{ '/img/screenshot_masterMetrics.png' | relativize_url }})

这一部分显示了master的所有度量信息，包括：

* **Master整体指标** 集群的整体度量信息。

* **逻辑操作** 执行的操作数量。

* **RPC调用** 每个操作的RPC调用次数。

## Alluxio Workers Web界面

每个Alluxio worker也提供Web界面显示worker信息。worker Web界面的默认端口是30000，访问
`http://<WORKER IP>:30000`即可查看。举个例子，如果你在本地启动Alluxio，访问
[localhost:30000](http://localhost:30000)即可查看worker Web界面。

### 主页

Alluxio worker的主页和Alluxio master类似，但是显示的是单个worker的特定信息。因此，它有类似的部分：
**Worker 概要**, **存储使用概要**, **分层存储详细信息**.

![workerHome]({{ '/img/screenshot_workerOverview.png' | relativize_url }})

### 块信息页面

在块信息页面，可以看到worker上的文件，以及其他信息，如：文件大小和文件所在的存储层。当你点击文件时，可以看
到文件的所有块信息。

![workerBlockInfo]({{ '/img/screenshot_workerBlockInfo.png' | relativize_url }})

### Worker度量信息

点击导航栏中的“Metrics”选项卡即可浏览worker的度量信息。

![workerMetrics]({{ '/img/screenshot_workerMetrics.png' | relativize_url }})

这一部分显示了worker的所有度量信息，包括：

* **Worker整体指标** Worker的整体度量信息。

* **逻辑操作** 执行的操作数量。
