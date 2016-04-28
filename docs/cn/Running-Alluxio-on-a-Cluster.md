---
layout: global
title: 在集群上独立运行Alluxio
nickname: 在集群上独立运行Alluxio
group: User Guide
priority: 2
---

## 独立集群

下载`Alluxio` tar文件并解压：

{% include Running-Alluxio-on-a-Cluster/download-extract-Alluxio-tar.md %}

在`alluxio/conf`目录下，将`alluxio-env.sh.template`拷贝到`alluxio-env.sh`。确保`JAVA_HOME`指向有效的Java 7安装路径。将`ALLUXIO_MASTER_ADDRESS`更新为运行Alluxio Master的机器的主机名。添加所有worker节点的IP地址到`alluxio/conf/workers`文件。最后，同步worker节点的所有信息。可使用

{% include Running-Alluxio-on-a-Cluster/sync-info.md %}

同步所有文件和文件夹到`alluxio/conf/workers` 文件中指定的主机里。

现在可以启动Alluxio：

{% include Running-Alluxio-on-a-Cluster/start-Alluxio.md %}

验证Alluxio是否运行，可以访问`http://<alluxio_master_hostname>:19999`，或查看`alluxio/logs`文件夹下的日志。也可以运行一个样例程序

{% include Running-Alluxio-on-a-Cluster/run-tests.md %}

**提示**: 如果使用EC2，确保master节点的安全组设置中允许Alluxio web UI端口上的连接。

## 使用bootstrap-conf参数的bin/alluxio脚本

Alluxio脚本包含创建集群基本配置的选项。运行：

{% include Running-Alluxio-on-a-Cluster/bootstrap-conf.md %}

并且`alluxio/conf/alluxio-env.sh`文件不存在的话，脚本会创建一个包含集群正确设置的`alluxio/conf/alluxio-env.sh`文件，集群的master节点运行在`<alluxio_master_hostname>`。

该脚本需要在每一个你想要配置的节点上执行。

脚本默认配置所有worker使用worker上总内存的2/3，该数量可以在worker上创建的`alluxio/conf/alluxio-env.sh`文件中修改。

## EC2集群上使用Spark
如果使用Spark启动EC2集群，`Alluxio`会默认被安装和配置。
