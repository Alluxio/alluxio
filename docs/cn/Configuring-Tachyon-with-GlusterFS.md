---
layout: global
title: 在GlusterFS上配置Tachyon 
nickname: Tachyon使用GlusterFS
group: Under Store
priority: 2
---

该向导介绍如何配置Tachyon从而使用[GlusterFS](http://www.gluster.org/)作为底层文件系统。

# 初始步骤

首先，本地要有Tachyon二进制包。你可以自己[编译Tachyon](Building-Tachyon-Master-Branch.html)，或者[下载二进制包](Running-Tachyon-Locally.html)

然后，由template文件创建配置文件：

{% include Configuring-Tachyon-with-GlusterFS/copy-tachyon-env.md %}

# 配置Tachyon

假定GlusterFS bricks与Tachyon部署在同样的节点上，且GlusterFS volume挂载在`/tachyon_vol`，那以下的环境变量要添加到`conf/tachyon-env.sh`配置文件中：

{% include Configuring-Tachyon-with-GlusterFS/underfs-address.md %}

# 使用GlusterFS在本地运行Tachyon 

配置完成后，你可以在本地启动Tachyon，观察是否正确运行：

{% include Configuring-Tachyon-with-GlusterFS/start-tachyon.md %}

该命令应当会启动一个Tachyon master和一个Tachyon worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master Web UI。

接着，你可以运行一个简单的示例程序：

{% include Configuring-Tachyon-with-GlusterFS/runTests.md %}

运行成功后，访问你的GlusterFS volume，确认其中包含了由Tachyon创建的文件和目录。在该测试中，创建的文件名称应像下面这样：

{% include Configuring-Tachyon-with-GlusterFS/glusterfs-file.md %}

运行以下命令停止Tachyon：

{% include Configuring-Tachyon-with-GlusterFS/stop-tachyon.md %}
